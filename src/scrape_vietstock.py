import os
import time
import random
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from utils import get_logger, load_config

# Khởi tạo Logger và Cấu hình
logger = get_logger("Vietstock_Scraper")
config = load_config()

class VietstockScraper:
    def __init__(self):
        self.session = self._init_session()
        self.headers = {
            "User-Agent": random.choice(config['scraping']['user_agents']),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive"
        }

    def _init_session(self):
        """Khởi tạo session với chiến lược Retry tự động."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2, # Nghỉ 2s, 4s, 8s giữa các lần thử
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    def fetch_html(self, ticker):
        """Tải mã nguồn HTML của trang ban lãnh đạo."""
        url = f"https://finance.vietstock.vn/{ticker.lower()}/ban-lanh-dao.htm"
        try:
            response = self.session.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"[{ticker}] Lỗi khi tải trang: {e}")
            return None

import re

def parse_vietstock_table(html_content, ticker):
    """Phân tích bảng HTML và tự động trích xuất sàn giao dịch từ thẻ tiêu đề."""
    if not html_content:
        return []
        
    soup = BeautifulSoup(html_content, "lxml")
    
    # --- LOGIC TRÍCH XUẤT SÀN TỰ ĐỘNG ---
    detected_exchange = "default" # Mặc định
    
    # Cách 1: Tìm trong thẻ <p class="title-intro-stock"> (Ảnh 1)
    intro_stock = soup.find("p", class_="title-intro-stock")
    
    # Cách 2: Tìm trong thẻ <span class="h4 title"> (Ảnh 2)
    span_title = soup.find("span", class_="h4 title")
    
    if intro_stock:
        text = intro_stock.get_text(strip=True)
        # Dùng regex lấy chữ trong ngoặc đơn: VIN (UpCOM) -> UpCOM
        match = re.search(r'\((.*?)\)', text)
        if match:
            detected_exchange = match.group(1).upper()
    elif span_title:
        # Lấy text trong thẻ <b> bên trong span: (HOSE: FPT) -> HOSE
        text = span_title.get_text(strip=True)
        # Tách lấy chữ trước dấu hai chấm hoặc trong ngoặc
        clean_text = re.sub(r'[^a-zA-Z]', ' ', text).split()
        for word in clean_text:
            if word.upper() in ['HOSE', 'HNX', 'UPCOM']:
                detected_exchange = word.upper()
                break
    # -----------------------------------

    table = soup.find("table", class_="table-striped")
    if not table:
        return []

    processed_data = []
    rows = table.find("tbody").find_all("tr")
    
    for row in rows:
        cols = row.find_all("td")
        if not cols: continue
            
        is_group_start = (len(cols) == 7)
        name_idx = 1 if is_group_start else 0
        role_idx = 2 if is_group_start else 1
        
        try:
            name = cols[name_idx].get_text(strip=True)
            role = cols[role_idx].get_text(strip=True)
            
            if name and role:
                processed_data.append({
                    "ticker": ticker.upper(),
                    "exchange": detected_exchange, # ĐÃ CÓ GIÁ TRỊ THỰC
                    "person_name": name,
                    "role": role,
                    "source": "vietstock",
                    "scraped_at": datetime.now().isoformat()
                })
        except IndexError:
            continue
            
    return processed_data
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
def main():
    tickers = config['scraping']['tickers']
    scraper = VietstockScraper()
    all_records = []

    logger.info(f"Bắt đầu thu thập dữ liệu Vietstock cho {len(tickers)} mã.")

    for ticker in tickers:
        logger.info(f"Đang xử lý: {ticker}")
        
        html = scraper.fetch_html(ticker)
        records = parse_vietstock_table(html, ticker)
        
        if records:
            all_records.extend(records)
            # Lấy exchange từ bản ghi đầu tiên (giả định tất cả các bản ghi đều cùng sàn)
            detected_exchange = records[0]['exchange']
            logger.info(f"[{ticker}] Thành công (Sàn {detected_exchange}): Lấy được {len(records)} nhân sự.")
        else:
            logger.warning(f"[{ticker}] Không tìm thấy dữ liệu hoặc bị chặn.")

        # Tuân thủ độ trễ lịch sự (Minimum 2s + Jitter)
        time.sleep(random.uniform(2.0, 4.0))

    # Lưu kết quả ra file Parquet
    if all_records:
        df = pd.DataFrame(all_records)
        import os
        output_path = config['paths']['raw_vietstock']
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info(f"HOÀN THÀNH: Đã lưu {len(df)} bản ghi vào {output_path}")
        
    save_to_processed(all_records, "vietstock")

if __name__ == "__main__":
    main()