# 📖 Từ Điển Dữ Liệu (Data Dictionary)

## 1. Thông tin tệp tin

- **Tên tệp:** `board_golden.parquet`
- **Định dạng:** Apache Parquet (Sử dụng Snappy compression).
- **Mục tiêu:** Cung cấp dữ liệu ban lãnh đạo sạch cho các bài toán phân tích quản trị.

## 2. Đặc tả các trường dữ liệu (Field Specifications)

| Tên trường           | Kiểu dữ liệu | Tỷ lệ Null | Ví dụ           | Mô tả & Ràng buộc                              |
| :------------------- | :----------- | :--------- | :-------------- | :--------------------------------------------- |
| **ticker**           | string       | 0%         | `ACB`           | Mã chứng khoán (Khóa chính).                   |
| **exchange**         | string       | 0%         | `HOSE`          | Sàn giao dịch (HOSE, HNX, UPCOM).              |
| **person_name**      | string       | 0%         | `Trần Hùng Huy` | Họ tên lãnh đạo (Đã xóa danh xưng Ông/Bà).     |
| **role**             | string       | 0%         | `CTHĐQT`        | Chức danh "Vàng" sau khi giải quyết xung đột.  |
| **source_agreement** | string       | 0%         | `both`          | Trạng thái nguồn (`both`, `conflict`, `only`). |
| **confidence_score** | float        | 0%         | `0.95`          | Điểm tin cậy tổng hợp (0.0 - 1.0).             |
| **merged_at**        | string       | 0%         | `2026-03-04...` | Thời điểm xử lý dữ liệu (ISO 8601).            |

## 3. Logic Điểm tin cậy (Confidence Score Logic)

Điểm số được tính toán dựa trên hệ thống trọng số đa tầng (Weighted Scoring):

1. **Name Matching (20%):** Độ khớp tên gốc (trước khi xóa dấu/danh xưng).
2. **Role Similarity (20%):** Độ khớp chức danh sau khi đã quy đổi về dạng viết tắt chuẩn.
3. **Source Agreement (40%):** Điểm thưởng lớn nếu thông tin xuất hiện ở cả CafeF và Vietstock.
4. **Data Completeness (20%):** Điểm cộng cho độ đầy đủ thông tin sàn và các trường phụ trợ.

### Giải thích giá trị (Caveats):

- **Score = 1.00:** Tuyệt đối tin cậy. Hai nguồn khớp hoàn toàn.
- **0.75 <= Score < 1.00:** Tin cậy cao. Người dùng có thể sử dụng cho phân tích định lượng nhưng cần lưu ý chức danh có thể sai khác nhẹ về ngôn từ.
- **Score < 0.50:** Bản ghi đơn nguồn. Cần đối soát thủ công với Báo cáo quản trị của doanh nghiệp nếu sử dụng cho các mục đích pháp lý.
