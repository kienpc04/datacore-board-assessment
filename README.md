🏦 Vietnam Financial Leadership Integration (VFLI)
Hệ thống tự động thu thập, chuẩn hóa và hợp nhất dữ liệu Hội đồng Quản trị & Ban điều hành từ các nguồn tin tài chính hàng đầu Việt Nam. Dự án giải quyết bài toán thực thể (Entity Resolution) để tạo ra bộ dữ liệu "Golden Record" tin cậy cho phân tích quản trị doanh nghiệp.

Prerequisites
Trước khi bắt đầu, hãy đảm bảo bạn đáp ứng các yêu cầu sau:

Đã cài đặt Python 3.10 hoặc phiên bản mới hơn.

Sử dụng hệ điều hành Windows, macOS hoặc Linux.

Có kiến thức cơ bản về Virtual Environments và Terminal/Command Prompt.

⚙️ Installation
Để cài đặt dự án, hãy thực hiện theo các bước sau:

Clone dự án hoặc giải nén folder:

Bash
cd datacore-board-assessment

Thiết lập môi trường ảo:

Bash
python -m venv venv

# Kích hoạt (Windows)

venv\Scripts\activate

# Kích hoạt (macOS/Linux)

source venv/bin/activate
Cài đặt các thư viện phụ thuộc:

Bash
pip install -r requirements.txt

🚀 Usage
Sau khi cài đặt thành công, bạn có thể vận hành Pipeline theo các bước sau:

Cào dữ liệu (Task 1 & 2):
Chạy các script để lấy dữ liệu từ CafeF và Vietstock.

Bash
python src/scrape_cafef.py
python src/scrape_vietstock.py

Hợp nhất dữ liệu (Task 3):
Xử lý logic so khớp thực thể và tạo file kết quả cuối cùng.

Bash
python src/merge.py

Kiểm thử hệ thống (Task 4):
Chạy bộ Unit Test để đảm bảo tính toàn vẹn của logic.

Bash
pytest -v

Phân tích dữ liệu:
Mở các tệp trong thư mục notebooks/ bằng Jupyter Notebook hoặc VS Code để xem báo cáo EDA.

🧠 Approach & Trade-offs
Kỹ thuật: Sử dụng Hybrid Fuzzy Matching để xử lý sự sai khác về danh xưng và lỗi Unicode giữa các nguồn.

Đánh đổi: Chấp nhận xóa bỏ hoàn toàn dấu cách và danh xưng trong giai đoạn so khớp để tối đa hóa tỷ lệ khớp (Match Rate), sau đó khôi phục định dạng chuẩn ở bản ghi cuối.

Hạn chế: Các mã cổ phiếu mới niêm yết (IPO) có thể gặp hiện tượng đơn nguồn (Single Source) do độ trễ cập nhật khác nhau.

🤝 Contributing
Để đóng góp cho dự án, hãy làm theo các bước sau:

Fork dự án này.

Tạo một Branch mới (git checkout -b feature/AmazingFeature).

Commit các thay đổi của bạn (git commit -m 'Add some AmazingFeature').

Push lên branch (git push origin feature/AmazingFeature).

Mở một Pull Request.

👥 Contributors
Nguyễn Trung Kiên - Lead Data Engineer - https://github.com/kienpc04

Cảm ơn đội ngũ Reviewer đã hỗ trợ định hướng kiến trúc hệ thống.

📚 Acknowledgments & References
Dữ liệu: Cung cấp bởi CafeF (cafef.vn) và Vietstock (vietstock.vn).

Thư viện: \* thefuzz: Hỗ trợ thuật toán Levenshtein Distance.

pandas: Xử lý cấu trúc dữ liệu bảng.

pytest: Khung kiểm thử tự động.

Tài liệu: Tham khảo quy trình xử lý Data Matching từ các blog kỹ thuật của ngành Data Engineering Việt Nam.
