# Customer Segmentation with Big Data

Đồ án môn **Big Data** về phân khúc khách hàng dựa trên dữ liệu giao dịch.

Hệ thống sử dụng **Hadoop HDFS** để lưu trữ dữ liệu và **Apache Spark / PySpark** để xử lý dữ liệu, xây dựng đặc trưng RFM và phân nhóm khách hàng bằng K-Means.

---

## 1. Công nghệ

| Thành phần          | Công nghệ               |
| ------------------- | ----------------------- |
| Storage             | Hadoop HDFS             |
| Processing          | Apache Spark / PySpark  |
| Programming         | Python                  |
| Clustering          | K-Means                 |
| Feature Engineering | RFM                     |
| Container           | Docker / Docker Compose |
| Visualization       | HTML, CSS, JavaScript   |
| Version Control     | Git / GitHub            |

### Yêu cầu

* Windows / Linux
* Docker Desktop
* Python 3.10+
* Git

---

## 2. Cài đặt

### Clone repository

```powershell
Clone git về 
cd BigData_Customer-Segmentation
```

### Cài Python dependencies

```powershell
pip install -r requirements.txt
```

Kiểm tra môi trường:

```powershell
python --version
docker --version
docker compose version
```

---

## 3. Khởi động hệ thống

Khởi động Hadoop và Spark:

```powershell
docker compose up -d
```

Kiểm tra các container:

```powershell
docker compose ps
```

Khởi tạo cấu trúc thư mục trên HDFS:

```powershell
python scripts\init_hdfs.py
```

Upload dữ liệu lên HDFS:

```powershell
python scripts\upload_to_hdfs.py
```

Sau khi dữ liệu được upload, thực hiện pipeline:

```text
Preprocessing
      ↓
RFM
      ↓
K-Means
      ↓
Evaluation
      ↓
Cluster Analysis
```

Các thành phần tương ứng nằm trong:

```text
src/preprocessing/
src/feature_engineering/
src/clustering/
src/analysis/
```

---

## 4. Web Demo

Project có một web demo dùng để trình bày các kết quả đã được tạo từ pipeline.

Web demo **không chạy lại Spark hoặc K-Means**. Nó chỉ đọc các file kết quả và biểu đồ đã được sinh ra.

Chạy web:

```powershell
python scripts\run_web_demo.py
```

Mở trình duyệt:


```text
http://127.0.0.1:8000/web/
```

Web demo gồm:

* Tổng quan dữ liệu
* Phân tích RFM
* Phân nhóm khách hàng
* Đánh giá K-Means
* Biểu đồ kết quả
* Kiến trúc HDFS + Spark

### Các giao diện dùng khi demo

| Thành phần        | Địa chỉ                    |
| ----------------- | -------------------------- |
| Web Demo          | http://127.0.0.1:8000/web/ |
| HDFS NameNode     | http://localhost:9870      |
| Spark Master      | http://localhost:8080      |
| Spark Application | http://localhost:4040      |

`4040` chỉ xuất hiện khi một Spark job đang chạy.

---

# Nội dung đề tài

## 5. Bài toán

Dữ liệu giao dịch của một cửa hàng có thể chứa số lượng lớn hóa đơn và thông tin mua hàng của khách hàng.

Mục tiêu của đề tài:

* Lưu trữ dữ liệu bằng HDFS.
* Xử lý dữ liệu bằng Apache Spark.
* Làm sạch dữ liệu giao dịch.
* Xây dựng các chỉ số RFM cho từng khách hàng.
* Phân nhóm khách hàng bằng K-Means.
* Phân tích đặc điểm của từng nhóm.
* Đề xuất một số hướng chăm sóc khách hàng dựa trên kết quả phân nhóm.

---

## 6. Dataset

Sử dụng bộ dữ liệu **Online Retail II** từ UCI Machine Learning Repository.

Dataset chứa thông tin giao dịch bán hàng với các trường chính:

```text
InvoiceNo
StockCode
Description
Quantity
InvoiceDate
UnitPrice
CustomerID
Country
```

Dữ liệu gốc có khoảng **1 triệu dòng**, kích thước khoảng **94 MB**.

> Dataset này chưa đạt quy mô Big Data nếu chỉ xét về kích thước. Đồ án tập trung vào việc xây dựng pipeline xử lý theo kiến trúc HDFS + Spark và khả năng mở rộng khi dữ liệu tăng lên.

---

## 7. Quy trình xử lý

```text
Online Retail II
       │
       ▼
    Raw CSV
       │
       ▼
     HDFS
       │
       ▼
Spark DataFrame
       │
       ▼
Data Cleaning
       │
       ▼
Processed Parquet
       │
       ▼
      RFM
       │
       ▼
Feature Scaling
       │
       ▼
    K-Means
       │
       ▼
Cluster Evaluation
       │
       ▼
Cluster Analysis
       │
       ▼
Visualization
```

---

## 8. Xử lý dữ liệu

Các bước làm sạch chính:

* Loại bỏ bản ghi trùng.
* Loại bỏ giao dịch bị hủy.
* Loại bỏ giao dịch có `Quantity <= 0`.
* Loại bỏ giao dịch có `UnitPrice <= 0`.
* Loại bỏ các bản ghi không có `CustomerID` khi xây dựng RFM.
* Kiểm tra dữ liệu ngày tháng và các giá trị không hợp lệ.
* Tính giá trị giao dịch.

```text
Amount = Quantity × UnitPrice
```

Dữ liệu sau xử lý được lưu dưới dạng **Parquet** trên HDFS.

---

## 9. RFM

Sau bước làm sạch, dữ liệu giao dịch được tổng hợp theo khách hàng.

### Recency

Số ngày kể từ lần mua hàng gần nhất đến ngày phân tích.

```text
Recency = AnalysisDate - LastPurchaseDate
```

### Frequency

Số lượng hóa đơn khác nhau của khách hàng.

### Monetary

Tổng giá trị giao dịch của khách hàng.

```text
Monetary = SUM(Amount)
```

Kết quả RFM:

```text
CustomerID
Recency
Frequency
Monetary
```

---

## 10. K-Means

Các đặc trưng RFM có thang đo khác nhau nên được chuẩn hóa trước khi đưa vào K-Means.

Thử nghiệm các giá trị:

```text
K = 2, 3, 4, 5, 6
```

Các mô hình được đánh giá bằng:

* Silhouette Score
* Số lượng khách hàng trong từng cluster

Kết quả hiện tại:

```text
Selected K = 4
Silhouette Score = 0.5325
```

---

## 11. Kết quả phân nhóm

Sau bước xây dựng RFM có **5.839 khách hàng**.

Với `K = 4`:

| Nhóm | Số khách hàng | Tỷ lệ | Đặc điểm RFM                                              |
| ---- | ------------: | ----: | --------------------------------------------------------- |
| 0    |         1.179 | 20,2% | Recency thấp, Frequency và Monetary cao                   |
| 1    |         1.442 | 24,7% | Recency cao, mức mua thấp hơn                             |
| 2    |         1.261 | 21,6% | Recency thấp, Frequency và Monetary ở mức trung bình/thấp |
| 3    |         1.957 | 33,5% | Frequency thấp, nhiều khách chỉ mua một lần               |

Tên nhóm trong phần phân tích được đặt dựa trên các đặc điểm RFM quan sát được. Dataset không cung cấp sẵn các nhãn này.

Một kết quả đáng chú ý:

> Khoảng 20% khách hàng thuộc nhóm giá trị cao nhưng tạo ra khoảng 73,5% doanh thu trong dữ liệu sau xử lý.

---

## 12. Phân tích khách hàng

Dựa trên các đặc điểm RFM, đồ án đưa ra một số hướng chăm sóc:

| Đặc điểm khách hàng            | Hướng đề xuất                  |
| ------------------------------ | ------------------------------ |
| Giá trị cao, mua thường xuyên  | Chăm sóc và duy trì            |
| Lâu không mua                  | Chương trình quay lại          |
| Khách mới, có khả năng mua lại | Khuyến khích mua lần tiếp theo |
| Mua một lần / giá trị thấp     | Tiếp cận với chi phí thấp      |

Các đề xuất trên được xây dựng từ kết quả phân tích dữ liệu và **chưa được kiểm chứng bằng A/B testing hoặc dữ liệu phản hồi thực tế**.

---

## 13. HDFS

Dữ liệu trên HDFS được tổ chức theo các thư mục:

```text
/data/customer-segmentation/
├── raw/
├── processed/
├── rfm/
├── output/
└── evaluation/
```

HDFS được sử dụng để lưu trữ dữ liệu đầu vào và các kết quả trung gian.

Spark đọc và ghi dữ liệu trực tiếp trên HDFS trong quá trình xử lý.

---

## 14. Cấu trúc project

```text
BigData_Customer-Segmentation/
│
├── configs/
│   └── config.yaml
│
├── docker/
│   ├── hadoop/
│   └── spark/
│
├── scripts/
│   ├── download_dataset.py
│   ├── profile_dataset.py
│   ├── init_hdfs.py
│   ├── upload_to_hdfs.py
│   └── ...
│
├── src/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── feature_engineering/
│   ├── clustering/
│   └── analysis/
│
├── web/
│   ├── index.html
│   ├── style.css
│   ├── app.js
│   └── cluster_interpretation.json
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## 15. Giới hạn của project

* Dataset khoảng 94 MB nên chưa thể hiện rõ vấn đề về quy mô Big Data nếu chỉ xét kích thước dữ liệu.
* Hệ thống chạy trên môi trường Docker local.
* Cấu hình hiện tại sử dụng một DataNode và một Spark Worker.
* HDFS replication hiện tại là 1.
* Các đặc trưng sử dụng chủ yếu là RFM.
* K-Means được thử nghiệm với `K = 2..6`.
* Kết quả phân nhóm chưa được kiểm chứng bằng dữ liệu thực tế từ các chiến dịch marketing.
* Các nhóm khách hàng và hướng chăm sóc là kết quả phân tích của đồ án, không phải nhãn có sẵn trong dataset.

---

## 16. Repository

GitHub:

https://github.com/Quang0408205/BigData_Customer-Segmentation
