# DMS & MISA ETL Pipeline Analysis

## 📌 Tổng quan dự án
Dự án xây dựng hệ thống tự động trích xuất, chuyển đổi và nạp dữ liệu (ETL) từ nhiều nguồn khác nhau (DMS, MISA, Excel) vào kho dữ liệu tập trung (Data Warehouse) trên PostgreSQL. Hệ thống phục vụ báo cáo phân tích hiệu suất tổng quan cho các chi nhánh khu vực, tình trạng nhân viên của từng phòng ban để lên kế hoạch tối ưu nhân sự.

## 🛠 Công nghệ sử dụng
* **Ngôn ngữ:** Python (Pandas, SQLAlchemy)
* **Cơ sở dữ liệu:** PostgreSQL, BigQuery
* **Công cụ BI:** Power BI, Apache Superset
* **Môi trường:** Docker, Git

## 🏗 Kiến trúc hệ thống
1. **Data Sources:** API DMS, Export file từ MISA, Google Sheets.
2. **Staging Area:** Dữ liệu thô được đẩy vào PostgreSQL (Schema: `stg`).
3. **Transformation:** Xử lý logic, làm sạch theo từng rules
4. **Data Warehouse:** Dữ liệu tinh gọn được lưu tại Schema `int` để phục vụ báo cáo và `mart` để dùng trong việc phân tích và đưa ra đánh giá nhanh các chỉ số.

## 🔐 Bảo mật dữ liệu (Data Privacy)
* Toàn bộ tên nhân viên, khách hàng và thông tin định danh đã được mã hóa bằng hàm **MD5**.
* Các thông tin tài chính đã được làm nhiễu (noise) để đảm bảo bí mật kinh doanh nhưng vẫn giữ nguyên logic tăng trưởng để phân tích.

## 📊 Kết quả (Dashboards) 
### Tổng quan
<img src="https://github.com/user-attachments/assets/6d079fa3-a8f1-4807-8cce-fbd463432a28" width="100%">

### Chi tiết sản phẩm - so sánh năm ngoái 

<img src="https://github.com/user-attachments/assets/5d1384d8-fdcd-4ee5-a895-b63a0a90f46e" width="100%">


### Chi tiết theo phòng ban - (so sánh năm ngoái và chỉ tiêu năm hiện tại<sale forecase>)

<img src="https://github.com/user-attachments/assets/0d77bedd-c652-4932-93e4-ed5c753a1dcb" width="100%">

### Phân tích chi tiết RFM 

<img src="https://github.com/user-attachments/assets/67080bd6-7d11-4f1a-a1af-131b988cbe2d" width="100%">

### Phân tích nhân sự

<img src="https://github.com/user-attachments/assets/9d3caa1f-fd44-4677-8a47-5f792a4d3ce1" width="100%">

### Data Modeling (dùng bảng flat - tối ưu với bộ data nhỏ và tháng chốt công nợ của từng phòng ban)

<img src="https://github.com/user-attachments/assets/3f4dba93-cc7d-4612-91c8-9fdff7019e8d" width="100%">


*Link báo cáo tương tác  :*  [![Xem báo cáo](https://img.shields.io/badge/Báo_cáo-Power_BI-yellow?style=for-the-badge&logo=powerbi)](https://app.powerbi.com/view?r=eyJrIjoiZDBkNDEzZjEtOTE3Ni00MzZmLWE1OTEtMDlhN2U1NjIyMmM0IiwidCI6ImRmODU3YTMzLTA3MTQtNGIwNi04MTMyLWU0ODVjMjMyZTJhMiJ9)

## 🚀 Cách cài đặt
Updating...
