# 06 — Feature Scaling + K-Means

> Phase 6+7 (2026-10-01). Mọi con số lấy từ lần chạy thật trên cluster
> (`output/reports/kmeans_report.json`, `output/evaluation/k_evaluation.json`, `output/reports/clustering_check.json`).
> Cách chọn K và đánh giá: `docs/07_evaluation.md`.

## 1. Pipeline

```text
HDFS rfm/ (5,839 khách: CustomerID, Recency, Frequency, Monetary)
   │  log1p(R), log1p(F), log1p(M)          ← giảm lệch phải
   │  VectorAssembler → raw_features        ← MLlib cần 1 cột vector
   │  StandardScaler(withMean, withStd)     ← z = (x − mean) / std
   │  KMeans(k = 4, seed = 42, k-means||, maxIter 100, tol 1e-4)
   ▼
HDFS output/clustering/ (CustomerID, Recency, Frequency, Monetary, Cluster)   ← R/F/M GỐC + nhãn cụm
```

Code: `src/clustering/kmeans.py` (`scale_features`, `train_kmeans`, `main` chạy K đã chọn),
`src/clustering/evaluate.py` (`silhouette_score`, `evaluate_k_range`, `main` so sánh K = 2..6). Tất cả là **Spark MLlib**.
Thư viện **numpy** được thêm vào image Spark (`docker/spark/Dockerfile`, numpy 2.2.6) vì `pyspark.ml` cần numpy.

## 2. Feature scaling

### Vì sao R, F, M có thang đo khác nhau?

Ba đặc trưng đo ba đại lượng khác đơn vị (số liệu thật Phase 5):

| | Đơn vị | Khoảng giá trị | std |
|---|---|---|---:|
| Recency | ngày | 1 – 739 | 208.61 |
| Frequency | số hóa đơn | 1 – 369 | 12.64 |
| Monetary | tiền | 2.90 – 579,128.64 | 13,900.43 |

### K-Means (distance-based) bị ảnh hưởng thế nào?

K-Means gán khách vào centroid **gần nhất theo khoảng cách Euclid**:
`d(x, c) = sqrt((R−R_c)² + (F−F_c)² + (M−M_c)²)`.
Nếu không scale, chênh lệch Monetary (hàng nghìn) lấn át hoàn toàn chênh lệch Frequency (vài đơn vị): hai khách khác nhau
100 lần mua nhưng cùng chi tiêu sẽ bị coi là "gần nhau". Khi đó K-Means thực chất chỉ phân cụm theo Monetary.

### Scaling thay đổi dữ liệu ra sao?

**StandardScaler** (`withMean=True, withStd=True`): `z = (x − mean) / std` → mỗi đặc trưng có **mean 0, std 1**.
1 đơn vị trên mọi trục = 1 độ lệch chuẩn → ba đặc trưng có trọng số ngang nhau trong khoảng cách.
Scaling **không đổi thứ tự** khách trên từng đặc trưng, chỉ đổi thang đo.

**log1p trước StandardScaler:** chỉ StandardScaler chưa đủ vì F, M **lệch phải rất mạnh** (skew 11.96 và 26.61).
Sau StandardScaler, khách lớn nhất vẫn cách trung bình **28.7 std (F)** và **41.5 std (M)** → K-Means tách riêng vài outlier.
`log1p(x) = ln(1 + x)` nén giá trị lớn, giữ thứ tự, xác định được tại 0.

Số liệu thật (mean / std):

| Đặc trưng | Gốc | Sau log1p | Sau StandardScaler (min … max) — có log1p | Không log1p (min … max) |
|---|---|---|---|---|
| Recency | 200.91 / 208.61 | 4.4699 / 1.5292 | 0 / 1 (−2.47 … 1.40) | 0 / 1 (−0.96 … 2.58) |
| Frequency | 6.22 / 12.64 | 1.5447 / 0.8060 | 0 / 1 (−1.06 … 5.42) | 0 / 1 (−0.41 … **28.69**) |
| Monetary | 2,829.53 / 13,900.43 | 6.7952 / 1.3788 | 0 / 1 (−3.94 … 4.70) | 0 / 1 (−0.20 … **41.46**) |

Kết quả thực nghiệm so sánh 2 cách (chi tiết `07_evaluation.md`): **không log1p** → với K = 3..6 luôn xuất hiện cụm chỉ
**2–21 khách** (outlier). **Có log1p** → cụm nhỏ nhất ≥ 7.7% số khách. → **Chọn log1p + StandardScaler**
(`clustering.log_transform: true`).

## 3. K-Means

| Khái niệm | Giải thích | Trong project |
|---|---|---|
| **Centroid** | "Tâm" của một cụm = trung bình tọa độ các điểm thuộc cụm | 4 centroid trong không gian 3 chiều đã scale |
| **Euclidean distance** | Khoảng cách đường thẳng giữa 2 điểm: căn tổng bình phương chênh lệch từng chiều | Đo độ "giống nhau" giữa khách và centroid |
| **Initialization** | Chọn K centroid ban đầu | `k-means||` (bản song song của k-means++: chọn tâm xa nhau), `seed = 42` để tái lập |
| **Assignment** | Mỗi khách được gán vào centroid gần nhất | Bước E, chạy song song trên các partition |
| **Centroid update** | Tính lại mỗi centroid = trung bình các khách vừa được gán | Bước M (reduce theo cụm) |
| **Iteration** | Lặp assignment → update | Tối đa `maxIter = 100` |
| **Convergence** | Dừng khi centroid gần như không dịch chuyển (< `tol = 1e-4`) | K = 4 **hội tụ sau 27 vòng** (< 100) |

Mục tiêu K-Means: tối thiểu **WSSSE** (tổng bình phương khoảng cách từ mỗi điểm tới centroid của nó). Mỗi vòng lặp không làm
WSSSE tăng → thuật toán chắc chắn hội tụ, nhưng có thể vào **cực tiểu cục bộ** (phụ thuộc khởi tạo) → cố định seed và dùng k-means||.

Trên Spark: dữ liệu chia partition trên executor; mỗi vòng, executor gán điểm và tính tổng cục bộ theo cụm, driver gộp lại thành
centroid mới rồi broadcast cho vòng sau.

## 4. Kết quả model cuối (K = 4)

| Thông số | Giá trị |
|---|---|
| Khách | 5,839 |
| Scaling | log1p + StandardScaler(withMean, withStd) |
| K / seed / init | 4 / 42 / k-means|| |
| numIter | **27** / 100 → hội tụ |
| WSSSE | 4,859.15 |
| Silhouette | **0.5325** |
| Thời gian | 15.0 s (scale + train + evaluate + ghi), 1 executor × 4 core |

Cụm (số thứ tự cụm do K-Means gán ngẫu nhiên theo khởi tạo, **không mang ý nghĩa**; **chưa đặt tên business** — Phase 8+10):

| Cluster | Khách | % | Centroid (z: logR, logF, logM) | TB Recency | TB Frequency | TB Monetary |
|---|---:|---:|---|---:|---:|---:|
| 0 | 1,179 | 20.2% | (−1.08, 1.51, 1.34) | 28.33 | 19.12 | 10,296.27 |
| 1 | 1,957 | 33.5% | (0.88, −0.88, −0.94) | 395.06 | 1.38 | 313.11 |
| 2 | 1,261 | 21.6% | (−0.89, −0.29, −0.22) | 29.03 | 3.03 | 837.64 |
| 3 | 1,442 | 24.7% | (0.46, 0.21, 0.38) | 228.81 | 5.05 | 1,881.63 |

Đọc centroid: z < 0 trên trục Recency = mua gần đây hơn trung bình; z > 0 trên F, M = mua nhiều/chi nhiều hơn trung bình.
Phân tích và diễn giải từng cụm thuộc Phase 8+10.

## 5. Output

- HDFS `hdfs://namenode:8020/data/customer-segmentation/output/clustering/` — 1 file Parquet, 76,297 bytes.
  Schema: `CustomerID` string · `Recency` int · `Frequency` bigint · `Monetary` double · `Cluster` int.
  Lưu **R/F/M gốc** (không lưu vector đã scale) để phân tích dễ hiểu.
- Báo cáo: `output/reports/kmeans_report.json` (scaling summary, centroid, sizes, numIter, WSSSE, silhouette).

## 6. Test (kết quả thật)

`scripts/check_clustering.py` → **RESULT: PASS (10/10)**:

| Check | Kết quả |
|---|---|
| `schema_matches` | 5 cột đúng kiểu |
| `customer_count_matches_rfm`, `customer_id_unique`, `no_nulls` | 5,839 khách = RFM, không trùng, không NULL |
| `k_clusters_all_non_empty` | Cluster ∈ {0, 1, 2, 3}, cụm nào cũng có khách |
| `cluster_sizes_match_report`, `cluster_sizes_match_evaluation` | 1,179 / 1,957 / 1,261 / 1,442 — giống hệt lần chạy K = 4 trong evaluate |
| `rfm_values_unchanged` | R/F/M trong output = bảng RFM (0 dòng khác) |
| `silhouette_matches_report` | Silhouette tính lại từ output = 0.5325 = report = k_evaluation → **tái lập được** với seed 42 |
| `kmeans_converged` | numIter 27 < maxIter 100 |

## 7. Cách chạy

```bash
# 1. So sánh K = 2..6 (2 cách scaling) -> output/evaluation/k_evaluation.{json,csv}
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/clustering/evaluate.py
# 2. Chọn K -> configs/config.yaml: clustering.selected_k (hiện = 4), rồi chạy model cuối
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/clustering/kmeans.py
# 3. Kiểm tra
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_clustering.py
```
