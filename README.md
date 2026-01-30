# Pipeline cho hệ thống tìm kiếm và phân tích các doanh nghiệp ở bảng Washington
## Tổng quan
- Dự án tập trung xây dựng một hệ thống kỹ thuật dữ liệu hiện đại, hỗ trợ thu thập, lưu trữ, xử lý và phân tích dữ liệu review và meta của doanh nghiệp qua các năm, nhằm góp phần ra quyết định của người dùng.

- Pipeline dữ liệu được tự động hóa toàn diện, ứng dụng các công nghệ Big Data tiên tiến như Apache Hadoop, Spark, Spark NLP và cơ sở dữ liệu phân tán, đảm bảo khả năng xử lý dữ liệu lớn hiệu quả, linh hoạt và có thể mở rộng theo thời gian thực.

- Nguồn dữ liệu: [Google Local Reviews (2021)](https://mcauleylab.ucsd.edu/public_datasets/gdrive/googlelocal/)

- Đây là bộ dữ liệu Google Local Reviews (2021), được thu thập và công
bố bởi nhóm nghiên cứu thuộc Đại học California, San Diego (UCSD). Đây là một trong
những tập dữ liệu học thuật lớn nhất và phong phú nhất về thông tin địa điểm và đánh giá của người dùng trên nền tảng Google Maps tại Hoa Kỳ.

----
## Cấu trúc thư mục

Gồm hai phần gồm cấu trúc thư mục thực hiện luồng xử lý dữ liệu và cấu trúc thư mục thực hiện cho việc deploy sản phẩm:

```
.
├── analysis_results
├── data
├── logs
├── README.MD
├── requirements.txt
├── source
│   ├── 01_prepare_data.py
│   ├── 02_upload_hdfs.py
│   ├── configs
│   │   └── settings.py
│   ├── deps_01.zip
│   ├── deps.zip
│   ├── download_models.py
│   ├── jobs
│   │   ├── etl_metadata.py
│   │   ├── etl_reviews.py
│   │   ├── gold_metadata.py
│   │   ├── gold_reviews.py
│   │   ├── silver_metadata.py
│   │   └── silver_reviews.py
│   ├── libs.zip
│   ├── main.py
│   ├── modules
│   │   ├── aggregation.py
│   │   ├── extractor.py
│   │   ├── loader.py
│   │   ├── sentiment.py
│   │   └── transformer.py
│   ├── schemas
│   │   └── tables.py
│   ├── sql
│   │   ├── final_db.sql
│   └── utils
└── washington-recsys-backend
    ├── app
    │   ├── api
    │   │   ├── router.py
    │   │   └── routers
    │   │       └── business.py
    │   ├── core
    │   │   ├── config.py
    │   ├── database.py
    │   ├── main.py
    │   ├── models
    │   │   ├── business.py
    │   │   ├── review.py
    │   │   └── stats.py
    │   ├── schemas
    │   │   ├── filters.py
    │   │   └── responses.py
    │   └── services
    │       ├── business_service.py
    │       └──  logger_service.py
    └── Trequirements.txt
```
---
## Kiến trúc pipeline
Ảnh chèn sau khi push code lên github
<!-- <p align="center">
  <img src="https://raw.githubusercontent.com/trgtanhh04/End-to-End-MovieDB-Data-Engineering/main/imge/Data%20engineering%20architecture.png" width="100%" alt="airflow">
</p> -->
---
## Quy Trình Xử Lý Dữ Liệu
1. Thu thập dữ liệu và lưu trữ dữ liệu thô
- Dữ liệu lấy trực tiếp từ [link](https://mcauleylab.ucsd.edu/public_datasets/gdrive/googlelocal/) sẽ bao gồm hai file `review.json` và `metadata.json`. Dữ liệu sau khi tải về được lưu dưới dạng tệp JSON trong hệ thống tệp cục bộ. Đây là nguồn dữ liệu thô ban đầu phục vụ cho các bước xử lý tiếp theo.
2. Nạp Dữ Liệu Vào Data Lake (HDFS)
- Các tệp JSON sẽ được chuyển vào hệ thống Data Lake dựa trên nền tảng HDFS. Điều này cho phép lưu trữ dữ liệu khối lượng lớn, hỗ trợ khả năng truy xuất và xử lý phân tán hiệu quả.
3. ETL Cơ Bản và xử Lý Nâng Cao 
- Sau khi lưu trữ vào HDFS, hệ thống sử dụng Kafka để kích hoạt chuỗi xử lý ETL. Bao gồm:

    - Extract: Đọc dữ liệu từ HDFS.
    - Transform: Làm sạch, chuẩn hóa, xử lý định dạng dữ liệu (chuyển đổi kiểu dữ liệu, tách thể loại, chuẩn hóa thời gian,...).
    - Load: Lưu lại dữ liệu đã xử lý vào một thư mục HDFS mới 
- Apache Spark được tích hợp để xử lý nâng cao dữ liệu, ví dụ:
    - Lọc và phân loại review theo điểm đánh giá
    - Phân tích doanh nghiệp dựa vào các review và thời gian\
    - Sentiment text rivew, classification group category.
    - Tạo các bảng tổng hợp phục vụ phân tích


# Terminal 1 - Backend
cd ~/bigdata/washington-recsys-backend
source .venv_be/bin/activate
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Terminal 2 - Frontend
cd ~/bigdata/washington-recsys-backend/washington-frontend
npm start

