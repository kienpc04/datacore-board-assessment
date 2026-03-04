import pandas as pd

def test_detailed_data(file_path):
    try:
        df = pd.read_parquet(file_path)
        
        print("="*80)
        print(f"BÁO CÁO CHI TIẾT DỮ LIỆU VÀNG (GOLDEN DATASET)")
        print("="*80)

        # 1. Thống kê tổng quan các trường
        print("\n[1] THỐNG KÊ TRƯỜNG DỮ LIỆU:")
        stats = pd.DataFrame({
            'Kiểu dữ liệu': df.dtypes,
            'Số dòng Null': df.isnull().sum(),
            'Số giá trị Unique': df.nunique()
        })
        print(stats)

        # 2. Kiểm tra định dạng Tên và Chức vụ (10 bản ghi đầu)
        print("\n[2] KIỂM TRA ĐỊNH DẠNG TÊN & CHỨC VỤ (10 MẪU ĐẦU):")
        # Kiểm tra xem tên đã mất chữ "Ông/Bà" chưa và chức danh có chuẩn không
        sample_cols = ['ticker', 'person_name', 'role', 'source_agreement', 'confidence_score']
        print(df[sample_cols].head(10).to_string(index=False))

        # 3. Phân tích các trường hợp Xung đột (Conflict)
        print("\n[3] DANH SÁCH CÁC TRƯỜNG HỢP XUNG ĐỘT CHỨC VỤ (Role Conflict):")
        # Đây là các dòng mà CafeF và Vietstock lệch chức danh nhau
        conflicts = df[df['source_agreement'] == 'conflict'].head(5)
        if not conflicts.empty:
            print(conflicts[sample_cols].to_string(index=False))
        else:
            print("  - Không có xung đột chức vụ nào (Hoặc logic match chưa tìm thấy).")

        # 4. Kiểm tra các mã niêm yết trên UPCOM (Kiểm chứng Task 2)
        print("\n[4] KIỂM TRA DỮ LIỆU MÃ SÀN UPCOM (VD: VIN):")
        # Chứng minh logic bắt sàn qua API đã hoạt động
        upcom_data = df[df['exchange'] == 'UPCOM'].head(5)
        if not upcom_data.empty:
            print(upcom_data[sample_cols].to_string(index=False))
        else:
            print("  - Cảnh báo: Không tìm thấy mã nào thuộc sàn UPCOM.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == "__main__":
    test_detailed_data("data/final/board_golden.parquet")
import pandas as pd

def view_conflicts(golden_path, cafef_path, vs_path):
    try:
        # Đọc dữ liệu từ 3 nguồn
        df_golden = pd.read_parquet(golden_path)
        df_cf = pd.read_parquet(cafef_path)
        df_vs = pd.read_parquet(vs_path)

        # Lọc ra các bản ghi có sự xung đột chức vụ
        conflict_list = df_golden[df_golden['source_agreement'] == 'conflict']
        
        print("="*100)
        print(f"PHÂN TÍCH CHI TIẾT XUNG ĐỘT (Tổng cộng: {len(conflict_list)} trường hợp)")
        print("="*100)
        print(f"{'Ticker':<8} | {'Nhân sự':<25} | {'Chức vụ CafeF':<30} | {'Chức vụ Vietstock':<30}")
        print("-" * 100)

        for _, row in conflict_list.head(20).iterrows():
            ticker = row['ticker']
            name = row['person_name']
            
            # Tìm chức vụ gốc từ file processed của CafeF và Vietstock
            # Lưu ý: Cần xử lý logic tìm kiếm tương tự join_key nếu tên không khớp 100%
            role_cf = df_cf[(df_cf['ticker'] == ticker) & (df_cf['person_name'].str.contains(name, na=False))]['role'].values
            role_vs = df_vs[(df_vs['ticker'] == ticker) & (df_vs['person_name'].str.contains(name, na=False))]['role'].values
            
            val_cf = role_cf[0] if len(role_cf) > 0 else "N/A"
            val_vs = role_vs[0] if len(role_vs) > 0 else "N/A"
            
            print(f"{ticker:<8} | {name:<25} | {val_cf[:30]:<30} | {val_vs[:30]:<30}")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == "__main__":
    view_conflicts(
        "data/final/board_golden.parquet",
        "data/processed/cafef_processed.parquet",
        "data/processed/vietstock_processed.parquet"
    )