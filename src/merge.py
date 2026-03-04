import pandas as pd
import numpy as np
import re
from datetime import datetime
from fuzzywuzzy import fuzz 
from unidecode import unidecode
from utils import get_logger, load_config

logger = get_logger("Golden_Merge")
config = load_config()

class GoldenMerger:
    def __init__(self):
        self.df_cf = pd.read_parquet(config['paths']['processed_cafef'])
        self.df_vs = pd.read_parquet(config['paths']['processed_vietstock'])

    def normalize_name(self, name):
        if not name or pd.isna(name): return ""
        # 1. Chuyển về chữ thường và xóa dấu ngay từ đầu để dễ match Regex
        name = unidecode(name.lower())
        
        # 2. Danh sách danh xưng đã xóa dấu
        honorifics = [
            'ong', 'ba', 'anh', 'chi', 't.s', 'ts', 'ths', 'th.s', 
            'gs', 'p.gs', 'pgs', 'ky su', 'cu nhan', 'tien si'
        ]
        
        # Sử dụng Regex để xóa các từ này nếu chúng đứng ở ĐẦU chuỗi
        pattern = r'^(' + '|'.join([re.escape(h) for h in honorifics]) + r')[\s.]+'
        name = re.sub(pattern, '', name)
        
        # 3. Xóa ký tự đặc biệt và khoảng trắng thừa
        name = re.sub(r'[^a-z\s]', '', name)
        return " ".join(name.split())

    def clean_display_name(self, name):
        """Làm sạch tên hiển thị nhưng giữ lại dấu tiếng Việt."""
        if not name or pd.isna(name): return "N/A"
        clean = re.sub(r'^(Ông|Bà|Anh|Chị|GS|P\.GS|T\.S|ThS|CN|Kỹ sư|Cử nhân)\.?\s+', 
                       '', str(name), flags=re.IGNORECASE)
        return clean.strip().title()

    def normalize_role(self, role):
        if not role or pd.isna(role): 
            return ""
        
        # 1. Chuyển về chữ thường, xóa dấu và khoảng trắng thừa
        role = unidecode(str(role).lower().strip())
        
        # 2. Bảng ánh xạ các cụm từ cần chuẩn hóa (dựa trên dữ liệu xung đột thực tế)
        replacements = {
            # Chuẩn hóa chức danh cấp cao
            r'\bthanh vien hoi dong quan tri\b': 'tvhdqt',
            r'\bthanh vien hdqt\b': 'tvhdqt',
            r'\bchu tich hoi dong quan tri\b': 'cthdqt',
            r'\bchu tich hdqt\b': 'cthdqt',
            r'\bpho chu tich\b': 'pho ct',
            r'\btong giam doc\b': 'tgd',
            r'\bpho tong giam doc\b': 'pho tgd',
            r'\bpho tong gd\b': 'pho tgd',
            r'\bke toan truong\b': 'ktt',
            r'\bban dieu hanh\b': 'bdh',
            r'\bthanh vien h d q t\b': 'tvhdqt',
            
            # Loại bỏ các từ bổ trợ gây nhiễu khi so khớp
            r'\bdoc lap\b': '',
            r'\bthuong truc\b': '',
            r'\bcao cap\b': '',
            r'\bchuyen trach\b': '',
        }
        
        # Thực hiện thay thế theo Regex
        for pattern, replacement in replacements.items():
            role = re.sub(pattern, replacement, role)
        
        # 3. Loại bỏ các ký tự đặc biệt (chỉ giữ lại chữ cái, số và dấu / cho chức danh kép)
        role = re.sub(r'[^a-z0-9/]', '', role)
        
        # 4. Sắp xếp lại các chức danh nếu có dấu / (Ví dụ: 'tgd/tvhdqt' và 'tvhdqt/tgd' là một)
        if '/' in role:
            parts = sorted(list(set(role.split('/'))))
            role = '/'.join(parts)
            
        return role
    def calculate_smart_score(self, row):
        # 1. Name Match Quality 
        # So sánh tên thô trước khi chuẩn hóa để xem độ lệch (VD: "Nguyễn Văn A" vs "Nguyen Van A")
        name_score = fuzz.token_sort_ratio(str(row['person_name_cf']), str(row['person_name_vs'])) / 100.0
        
        # 2. Role Similarity 
        role_cf_norm = self.normalize_role(row['role_cf'])
        role_vs_norm = self.normalize_role(row['role_vs'])
        role_score = fuzz.token_set_ratio(role_cf_norm, role_vs_norm) / 100.0
        
        # 3. Source Agreement & Completeness 
        agreement_bonus = 0.4 if row['_merge'] == 'both' else 0.0
        
        # Kiểm tra độ đầy đủ (Không null các trường quan trọng)
        completeness = 0.2
        if pd.isna(row['role_cf']) or pd.isna(row['role_vs']):
            completeness = 0.1

        base_score = 0.2 if row['_merge'] != 'both' else 0.0
        # Tổng hợp điểm theo trọng số
        total_score = (name_score * 0.2) + (role_score * 0.2) + agreement_bonus + completeness + base_score
        
        # Gán nhãn Agreement dựa trên ngưỡng
        if row['_merge'] != 'both':
            agreement_label = "cafef_only" if row['_merge'] == 'left_only' else "vietstock_only"
        else:
            agreement_label = "both" if role_score > 0.95 else "conflict"
            
        return agreement_label, round(total_score, 2)

    def resolve_role(self, row):
        cf = str(row['role_cf']) if pd.notna(row['role_cf']) else ""
        vs = str(row['role_vs']) if pd.notna(row['role_vs']) else ""
        
        # Ưu tiên chuỗi có chứa ký tự đặc biệt '/' hoặc có độ dài lớn hơn
        # vì chúng thường mô tả đầy đủ các vai trò kiêm nhiệm
        if '/' in vs and '/' not in cf: return vs
        if '/' in cf and '/' not in vs: return cf
        
        return vs if len(vs) >= len(cf) else cf

    def process(self):
        # 1. Chuẩn hóa Key
        self.df_cf['join_key'] = self.df_cf['person_name'].apply(self.normalize_name)
        self.df_vs['join_key'] = self.df_vs['person_name'].apply(self.normalize_name)

        # 2. Outer Join
        merged = pd.merge(
            self.df_cf, self.df_vs, 
            on=['ticker', 'join_key'], 
            how='outer', 
            suffixes=('_cf', '_vs'), 
            indicator=True
        )

        # 3. Tính toán trường phái sinh
        merged['final_exchange'] = merged['exchange_cf'].fillna(merged['exchange_vs'])
        merged['final_name'] = merged['person_name_vs'].fillna(merged['person_name_cf']).apply(self.clean_display_name)
        
        # Áp dụng Smart Score
        metrics = merged.apply(lambda r: self.calculate_smart_score(r), axis=1)
        merged['source_agreement'] = metrics.apply(lambda x: x[0])
        merged['confidence_score'] = metrics.apply(lambda x: x[1])
        
        # Áp dụng Resolve Role (Sử dụng hàm thông minh bạn đã viết)
        merged['final_role'] = merged.apply(lambda r: self.resolve_role(r), axis=1)

        # 4. Tạo Golden DataFrame (Sử dụng các cột final_ đã xử lý)
        golden_df = pd.DataFrame({
            "ticker": merged['ticker'],
            "exchange": merged['final_exchange'], 
            "person_name": merged['final_name'],
            "role": merged['final_role'], # Sửa từ fillna sang final_role
            "source_agreement": merged['source_agreement'],
            "confidence_score": merged['confidence_score'],
            "merged_at": datetime.now().isoformat()
        })
        
        # Xóa trùng lặp bản ghi
        golden_df = golden_df.drop_duplicates(subset=['ticker', 'person_name', 'role'])
        
        return golden_df

def main():
    merger = GoldenMerger()
    df_golden = merger.process()
    
    output_path = config['paths'].get('final_golden', 'data/final/board_golden.parquet')
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df_golden.to_parquet(output_path, index=False)
    logger.info(f"Hoàn thành Golden Dataset: {len(df_golden)} bản ghi.")

if __name__ == "__main__":
    main()