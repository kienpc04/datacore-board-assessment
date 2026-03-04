
import os
import time
import random
import requests
import pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from utils import get_logger, load_config

# 1. Khởi tạo Logger và Cấu hình từ Phase 1
logger = get_logger("CafeF_Scraper")
config = load_config()

def create_robust_session():
    """Khởi tạo session với cơ chế Retry và Exponential Backoff."""
    session = requests.Session()
    
    # Cấu hình chiến lược Retry cho các lỗi mạng tạm thời hoặc Rate Limit (429)
    retry_strategy = Retry(
        total=3,                          # Thử lại tối đa 3 lần
        backoff_factor=1,                 # Đợi: 1s, 2s, 4s giữa các lần thử
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def fetch_board_data_api(ticker, session):
    """Truy vấn trực tiếp API với cơ chế an toàn."""
    # URL lấy từ phân tích Network Tab thực tế
    api_url = f"https://cafef.vn/du-lieu/Ajax/PageNew/ListCeo.ashx?Symbol={ticker.lower()}&PositionGroup=0"
    
    headers = {
        "User-Agent": random.choice(config['scraping']['user_agents']),
        "Referer": f"https://cafef.vn/du-lieu/hose/{ticker.lower()}-ban-lanh-dao-so-huu.chn",
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        # Session sẽ tự động xử lý Retry dựa trên cấu trúc đã mount ở trên
        response = session.get(api_url, headers=headers, timeout=config['scraping']['timeout'])
        response.raise_for_status()
        
        # Một số trường hợp API trả về string JSON thay vì dict, xử lý linh hoạt
        try:
            return response.json()
        except:
            import json
            return json.loads(response.text)
            
    except Exception as e:
        logger.error(f"[{ticker}] Lỗi không thể lấy dữ liệu: {str(e)}")
        return None

def process_records(raw_data, ticker, exchange):
    """Chuyển đổi dữ liệu thô sang Schema yêu cầu."""
    processed_list = []
    # Đảm bảo raw_data là dict và có trường 'Data'

    if not isinstance(raw_data, dict) or 'Data' not in raw_data:
        return []
    
    for group in raw_data['Data']:
        leaders = group.get('values', [])
        for leader in leaders:
            record = {
                "ticker": ticker.upper(),
                "exchange": exchange.upper(),
                "person_name": leader.get('Name', '').strip(),
                "role": leader.get('Position', '').strip(),
                "source": "cafef",
                "scraped_at": datetime.now().isoformat()
            }
            processed_list.append(record)
    return processed_list

import re
from bs4 import BeautifulSoup

import json
import json

def get_exchange_perfect(ticker, session, max_retries=2):
    """Xác định sàn qua API với cơ chế thử lại nếu kết quả rỗng hoặc lỗi."""
    for attempt in range(max_retries + 1):
        timestamp = int(time.time() * 1000)
        api_url = f"https://cafef.vn/du-lieu/ajax/pagenew/companyinfor.ashx?symbol={ticker.lower()}&_={timestamp}"
        
        headers = {
            "User-Agent": random.choice(config['scraping']['user_agents']),
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"https://cafef.vn/du-lieu/upcom/{ticker.lower()}-thong-tin-co-ban.chn"
        }

        try:
            response = session.get(api_url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                # Thử lấy từ trường 'San' hoặc trong 'Data.San'
                exchange = data.get('San', '') or data.get('Data', {}).get('San', '')
                exchange = exchange.strip().upper()

                if exchange:
                    if any(x in exchange for x in ['HOSE', 'HSX']): return 'HOSE'
                    if 'HNX' in exchange: return 'HNX'
                    if 'UPCOM' in exchange: return 'UPCOM'
                    return exchange

            # Nếu kết quả không như ý, đợi và thử lại
            if attempt < max_retries:
                wait_time = (attempt + 1) * 3 # Đợi 3s, 6s...
                logger.warning(f"[{ticker}] Sàn rỗng, thử lại lần {attempt + 1} sau {wait_time}s...")
                time.sleep(wait_time)

        except Exception as e:
            if attempt < max_retries:
                time.sleep(2)
                continue
    
    return "UNKNOWN"
def save_to_processed(all_data, source_name):
    """Làm sạch sơ bộ và lưu vào thư mục data/processed/."""
    if not all_data:
        logger.warning(f"Không có dữ liệu để lưu cho {source_name}")
        return

    df = pd.DataFrame(all_data)
    
    # 1. Chuẩn hóa tên sàn (Sử dụng kết quả từ Task 2 của bạn)
    # Đảm bảo cột exchange không có giá trị rỗng
    df['exchange'] = df['exchange'].fillna('UNKNOWN').str.upper()
    
    # 2. Làm sạch tên nhân sự (Viết hoa chữ cái đầu)
    if 'person_name' in df.columns:
        df['person_name'] = df['person_name'].str.strip().str.title()
        
    # 3. Tạo thư mục nếu chưa tồn tại
    processed_dir = "data/processed"
    os.makedirs(processed_dir, exist_ok=True)
    
    # 4. Lưu file Parquet
    file_path = f"{processed_dir}/{source_name}_processed.parquet"
    df.to_parquet(file_path, index=False)
    logger.info(f"--- Đã lưu file sạch vào: {file_path} ---")
# Cập nhật trong hàm main()
def main():
    tickers = config['scraping']['tickers']
    all_extracted_data = []
    session = create_robust_session()

            
    for ticker in tickers:
        max_retries = 2
        detected_exchange = get_exchange_perfect(ticker, session)
        for attempt in range(max_retries + 1):
            raw_json = fetch_board_data_api(ticker, session)
            records = process_records(raw_json, ticker, detected_exchange)
            
            if len(records) > 0:
                all_extracted_data.extend(records)
                logger.info(f"[{ticker}] Thành công (Sàn {detected_exchange}): {len(records)} nhân sự.")
                break
            else:
                if attempt < max_retries:
                    wait_time = (attempt + 1) * 5 # Tăng thời gian chờ: 5s, 10s
                    logger.warning(f"[{ticker}] Kết quả rỗng, thử lại lần {attempt + 1} sau {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{ticker}] Thất bại sau {max_retries} lần thử lại (0 nhân sự).")

    # Xuất dữ liệu ra file Parquet
    if all_extracted_data:
        df = pd.DataFrame(all_extracted_data)
        
        import os
        os.makedirs(os.path.dirname(config['paths']['raw_cafef']), exist_ok=True)
        
        df.to_parquet(config['paths']['raw_cafef'], index=False)
        logger.info(f"HOÀN THÀNH: Đã lưu {len(df)} bản ghi vào Parquet.")
    else:
        logger.error("Không có dữ liệu nào được thu thập thành công.")
    
    save_to_processed(all_extracted_data, "cafef")

if __name__ == "__main__":
    main()