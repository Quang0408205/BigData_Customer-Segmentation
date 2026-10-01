# Project Overview

**Đề tài:** Phân khúc khách hàng từ dữ liệu giao dịch lớn.

Từ lịch sử giao dịch của một cửa hàng bán lẻ trực tuyến (Online Retail II, ~1.07 triệu dòng),
project xây dựng pipeline Big Data: lưu dữ liệu trên HDFS, xử lý bằng Spark, tính đặc trưng
RFM cho từng khách hàng, phân cụm bằng K-Means, đánh giá bằng Silhouette Score và diễn giải
các cụm thành nhóm khách hàng có ý nghĩa kinh doanh.

Trạng thái: **hoàn thành** (2026-10-01) — toàn pipeline chạy end-to-end (13 bước, ~4.9 phút), mọi check PASS và
kết quả tái lập (raw 1,067,371 → processed 770,563 → RFM 5,839 khách → K = 4, Silhouette 0.5325 → profile + 7 biểu đồ).
Chuẩn bị bảo vệ: `docs/14_viva.md`.

Mục lục: Architecture · Dataset · HDFS · Spark · Preprocessing · RFM · Feature Scaling · K-Means · Evaluation (Silhouette) ·
Cluster Analysis · Visualization · Big Data characteristics · Limitations · How to Run.

# Architecture

Chi tiết: `docs/04_architecture.md`.

```text
data/raw (Windows) ──(upload_to_hdfs.py)──► HDFS  /data/customer-segmentation/raw/online_retail_II.csv
                                              │  hdfs://namenode:8020
                                              ▼
                     Spark driver (spark-submit trong container spark-master)
                                              │  spark://spark-master:7077
                                              ▼
                     spark-worker → executor (4 core) đọc block từ datanode
                                              │
                 processed / rfm / output / evaluation  (ghi lại vào HDFS)
```

**End-to-end (Phase 12, chi tiết `docs/04_architecture.md` mục 9):** 13 bước chạy tuần tự — 3 bước trên host
(`init_hdfs.py`, `upload_to_hdfs.py`, biểu đồ), 10 bước bằng `spark-submit` trong container. Tất cả exit 0, mọi check PASS;
output được tạo lại và giống hệt lần trước (7 PNG giống từng byte) → pipeline **chạy lại được, kết quả tái lập**.

4 container (Docker Compose): `namenode`, `datanode` (Hadoop 3.4.1), `spark-master`, `spark-worker` (Spark 4.0.4).
Code project được mount vào `/opt/project` trong container Spark → sửa code trên Windows, chạy trên cluster.
`local[*]` trên Windows (.venv) chỉ dùng ở Phase 1 (profile file local); pipeline chính đọc từ HDFS trên cluster.

# Dataset

Chi tiết: `docs/02_dataset.md`.

- **Nguồn:** UCI Machine Learning Repository, Online Retail II (id 502), tải bằng Python.
- **Quy mô thực tế:** 1,067,371 dòng × 8 cột, CSV 94.3 MB, 01/12/2009 → 09/12/2011.
- **Mỗi dòng** = một sản phẩm trong một hóa đơn (transaction line).
- **Cột:** Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country.
- **Cardinality:** 5,942 khách hàng, 53,628 hóa đơn, 43 quốc gia, 5,305 mã sản phẩm.

Phát hiện quan trọng:

1. **22.77% dòng thiếu Customer ID** (243,007) → không dùng được cho RFM.
2. **34,335 dòng trùng hoàn toàn**, trong đó 22,523 dòng do file Excel gốc có 2 sheet
   **chồng lấn 01–09/12/2010** (đã kiểm chứng: 22,523 dòng có ở cả hai sheet, giống hệt nhau).
3. **Hóa đơn hủy** (`C…`) 19,494 dòng; Quantity âm 22,950 dòng; Price = 0 có 6,202 dòng; Price âm 5 dòng (bad debt).
4. **Mã không phải sản phẩm** (POST, M, BANK CHARGES, AMAZONFEE…) 6,094 dòng, chứa các giá trị tiền lớn nhất.
5. Phân phối Quantity/Price **lệch phải rất mạnh** (p99 Quantity = 100, max = 80,995).

Quyết định kỹ thuật ở tầng raw:

- Excel → CSV (Spark đọc CSV native, đọc phân tán được); không sửa giá trị nào.
- Giữ nguyên tên cột gốc; mọi cột đọc dạng STRING; ép kiểu để ở preprocessing.
- `TIMESTAMP_NTZ` cho InvoiceDate (giờ địa phương, không timezone) — tránh lệch 7 tiếng khi collect về Python.

# HDFS

- **NameNode** giữ metadata (cây thư mục, file → block → DataNode); **DataNode** giữ block thật.
- **Block** 128 MB; **replication = 1** vì chỉ có 1 DataNode (không chịu lỗi — hạn chế của môi trường 1 máy).
- Thư mục project: `/data/customer-segmentation/{raw,processed,rfm,output,evaluation}`, tạo bằng `scripts/init_hdfs.py`,
  owner `spark` để Spark ghi được.
- Dữ liệu HDFS nằm trên Docker named volume → còn sau `docker compose down` (đã kiểm tra).
- Đã test: `dfsadmin -report` (1 live DataNode), `put` / `ls` / `fsck` (1 block, HEALTHY).

**Ingestion (Phase 3):** `scripts/upload_to_hdfs.py` (chạy trên host) đưa `data/raw/online_retail_II.csv` lên
`hdfs://namenode:8020/data/customer-segmentation/raw/online_retail_II.csv`:

- Kiểm tra file local (tồn tại, không rỗng, header 8 cột) → `docker compose cp` vào container namenode → `hdfs dfs -put`
  → xóa file tạm → so **size + sha256** HDFS với local → `fsck`.
- **Không upload trùng:** đã có bản giống hệt (cùng size + sha256) → `SKIP`; khác → báo lỗi, chỉ ghi đè khi `--force`.
- Kết quả thật: **94,268,848 bytes**, sha256 khớp, **1 block** (< 128 MB), replication 1, HEALTHY. Chạy lần 2 → SKIP.
- Raw local giữ nguyên; trên HDFS là bản sao byte-by-byte. Pipeline chỉ đọc raw, không ghi vào `raw/`.
- Raw giữ **CSV** (định dạng nguồn, so được bằng sha256). Processed dùng **Parquet** từ Phase 4: lưu theo cột (column pruning),
  có schema/kiểu dữ liệu, nén tốt, predicate pushdown (min/max theo row group), nhiều file `part-*` đọc/ghi song song.

# Spark

- Spark **Standalone**: Master (cấp tài nguyên) + Worker (chạy executor). Driver = chương trình `spark-submit`.
- Cluster: 1 worker, 4 core, 2 GB; executor 1 GB. Spark 4.0.4, Python 3.10.12 trong container.
- Spark đọc HDFS: driver hỏi NameNode vị trí block → chia split/partition → executor đọc trực tiếp từ DataNode.
- `src/ingestion/hdfs_loader.py`: `read_raw_transactions`, `read_parquet`, `path_exists`, `list_dir`, `delete_path`.
- Đã test (`scripts/spark_hdfs_smoke.py`): job phân tán cho kết quả đúng, đọc CSV từ HDFS (1,000 dòng),
  ghi Parquet lên HDFS và đọc lại.
- **Từ Phase 3, pipeline đọc raw qua `hdfs_loader.read_raw_transactions(spark)`** (HDFS), không đọc `data/raw/` local.
- Kiểm tra thật (`scripts/check_hdfs_ingestion.py`, 7/7 PASS, báo cáo `output/reports/hdfs_ingestion_check.json`):
  **1,067,371 dòng × 8 cột STRING**, 0 corrupt record, null Customer ID 243,007 / Description 4,382 (khớp Phase 1),
  5 dòng đầu khớp CSV local.
- **1 block nhưng 4 partition:** Spark chia theo split size = min(128 MB, max(4 MB, (94,268,848 + 4 MB)/4 core)) ≈ 23.5 MB
  → ceil(3.83) = **4 partition** = 4 task song song (đo thật bằng `df.rdd.getNumPartitions()`).

# Preprocessing

Chi tiết: `docs/03_preprocessing.md`. Code: `src/preprocessing/clean_transactions.py` (PySpark, chạy trên cluster).

- **Input:** raw CSV trên HDFS (1,067,371 dòng). **Output:** `hdfs://namenode:8020/data/customer-segmentation/processed/`
  — Parquet, 4 file, **770,563 dòng** (72.19%), 5,839 khách, 36,338 hóa đơn.
- Đổi tên cột (`InvoiceNo`, `UnitPrice`, `CustomerID`), ép kiểu bằng `try_cast` / `try_to_timestamp` (ANSI mode),
  tạo **`Amount = Quantity × UnitPrice`** = giá trị tiền của một dòng giao dịch → Monetary ở Phase 5 = tổng Amount.
- 9 rule, mỗi rule ghi detection · reason · handling · before · removed · after (`output/reports/preprocessing_report.json`):

| # | Rule | Removed |
|---|---|---:|
| 1 | InvoiceDate không hợp lệ | 0 |
| 2 | Giá trị không parse được / NULL | 0 |
| 3 | Dòng trùng 8 cột (gồm 22,523 dòng chồng lấn 2 sheet) | 34,335 |
| 4 | Hóa đơn hủy `C…` (19,104 dòng) + dòng mua bị hủy khớp 1-1 (6,146 dòng) | 25,250 |
| 5 | Hóa đơn `A…` (Adjust bad debt) | 6 |
| 6 | Quantity ≤ 0 | 3,393 |
| 7 | UnitPrice ≤ 0 | 2,621 |
| 8 | Thiếu CustomerID | 228,487 |
| 9 | StockCode không phải sản phẩm (25 mã tường minh: POST, DOT, C2, M, D, BANK CHARGES, …) | 2,716 |

- Quyết định quan trọng: hai dòng lớn nhất raw (80,995 và 74,215 sản phẩm) bị **hủy sau 12–16 phút** → bỏ cả dòng hủy
  và dòng mua khớp (cùng khách, mã, |số lượng|, giá) để Monetary không bị thổi phồng.
- Test (`scripts/check_processed_data.py`, đọc lại Parquet): **10/10 PASS** — 0 NULL, 0 trùng, 0 giá trị không hợp lệ,
  Amount = Quantity × UnitPrice, số dòng khớp báo cáo và khớp raw − tổng removed.
- Parquet 11.0 MB vs CSV 94.3 MB. Gộp `coalesce(4)` vì không gộp sẽ ra 200 file nhỏ (small files problem trên HDFS).
- Hạn chế: 12,244 dòng hủy không khớp được dòng mua (hủy một phần, hủy qua mã `M`…) → vài khách có Monetary hơi cao.

# RFM

Chi tiết: `docs/05_rfm.md`. Code: `src/feature_engineering/build_rfm.py` (Spark `groupBy(CustomerID).agg(...)`).

- **Transaction-level → customer-level:** 770,563 dòng giao dịch → **5,839 khách**, mỗi khách 1 vector (R, F, M).
  Phải aggregate vì phân khúc là theo *khách hàng*; K-Means cần mỗi điểm dữ liệu là một khách.
- **AnalysisDate = 2011-12-10** = ngày giao dịch cuối của processed (2011-12-09) + 1 ngày (rule trong config, không hard-code ngày).
  Lấy từ dữ liệu chứ không lấy ngày hiện tại vì dataset dừng ở 2011.
- **Recency** = `datediff(AnalysisDate, ngày mua gần nhất)` (ngày) · **Frequency** = `countDistinct(InvoiceNo)` (số lần mua,
  không phải số dòng) · **Monetary** = `sum(Amount)`.
- Thống kê: Recency 1–739 (median 96) · Frequency 1–369 (median 3; 27.6% khách chỉ mua 1 lần) ·
  Monetary 2.90–579,128.64 (median 851.01, mean 2,829.53). **Lệch phải mạnh:** skew F 11.96, M 26.61;
  top 1% khách chiếm 31.02% doanh thu. Outlier IQR: F 421 khách, M 616 khách — giữ lại (khách lớn thật).
- Tương quan: R–F −0.26, R–M −0.125, F–M 0.625.
- Output: `hdfs://namenode:8020/data/customer-segmentation/rfm/` (Parquet, 1 file, giá trị gốc chưa scale).
- Test (`scripts/check_rfm.py`): **11/11 PASS** — tính lại RFM bằng Spark SQL độc lập, so từng khách: 0 sai lệch;
  Σ Frequency = 36,338 hóa đơn; Σ Monetary = Σ Amount.

# Feature Scaling

Chi tiết: `docs/06_kmeans.md` mục 2. Code: `src/clustering/kmeans.py: scale_features()`.

- R, F, M khác đơn vị và thang đo (ngày 1–739; hóa đơn 1–369; tiền 2.90–579,128.64). K-Means dùng khoảng cách Euclid →
  không scale thì Monetary lấn át, K-Means thực chất chỉ chia theo tiền.
- **StandardScaler** `z = (x − mean)/std` → mỗi đặc trưng mean 0, std 1, trọng số ngang nhau.
- **log1p trước StandardScaler** vì F, M lệch rất mạnh (skew 11.96, 26.61): chỉ StandardScaler thì khách lớn nhất vẫn cách
  trung bình 28.7 std (F) và 41.5 std (M). Thực nghiệm: không log1p → K = 3..6 đều có cụm 2–21 khách (outlier);
  có log1p → cụm nhỏ nhất ≥ 7.7%.
- Pipeline MLlib: `log1p → VectorAssembler → StandardScaler(withMean, withStd)`.

# K-Means

Chi tiết: `docs/06_kmeans.md`. Code: `src/clustering/kmeans.py` (Spark MLlib `KMeans`).

- **Centroid** = tâm cụm (trung bình các điểm). **Assignment**: gán mỗi khách vào centroid gần nhất (Euclid).
  **Update**: tính lại centroid. **Iteration**: lặp 2 bước. **Convergence**: dừng khi centroid dịch < `tol` (1e-4) hoặc đạt `maxIter` (100).
- Khởi tạo `k-means||`, `seed = 42` (tái lập). K = 4 hội tụ sau **27 vòng**, WSSSE 4,859.15.
- Kết quả K = 4 (chưa đặt tên business): 1,179 khách (TB R 28 / F 19.1 / M 10,296) · 1,957 (395 / 1.4 / 313) ·
  1,261 (29 / 3.0 / 838) · 1,442 (229 / 5.1 / 1,882).
- Output: `hdfs://namenode:8020/data/customer-segmentation/output/clustering/` — `CustomerID, Recency, Frequency, Monetary, Cluster` (R/F/M gốc).
- Image Spark thêm **numpy** (bắt buộc cho `pyspark.ml`).

# Evaluation

Chi tiết: `docs/07_evaluation.md`. Code: `src/clustering/evaluate.py`.

- Metric: **Silhouette** (chính), **cluster size**, **processing time**; bổ sung WSSSE (elbow), numIter (hội tụ),
  TB R/F/M theo cụm (diễn giải).
- K = 2..6, log1p: Silhouette 0.6266 / 0.5060 / **0.5325** / 0.5145 / 0.4944; cụm nhỏ nhất 39.8% / 21.3% / 20.2% / 7.7% / 7.7%.
- **Chọn K = 4:** Silhouette cao nhất trong K = 3..6, cụm cân bằng (≥ 20%), 4 cụm khác nhau rõ trên R/F/M.
  K = 2 có Silhouette cao nhất nhưng chỉ chia "đang hoạt động / không hoạt động" — quá thô.
- Hạn chế Silhouette: ưu tiên cụm cầu, cao ở K nhỏ, phụ thuộc scaling/outlier (không log1p cho 0.77 với cụm 2 khách),
  tính trên không gian đã scale, không có ground truth.
- Test (`scripts/check_clustering.py`): **10/10 PASS** — Silhouette tính lại từ output = 0.5325 (tái lập), R/F/M không đổi.

# Cluster Analysis

Chi tiết: `docs/08_business_analysis.md`. Code: `src/analysis/cluster_analysis.py` (Spark), `src/analysis/charts.py` (Matplotlib, host).

- Profile tính bằng Spark từ output clustering (R/F/M gốc) + **Tenure** (ngày từ lần mua đầu, chỉ để mô tả) →
  HDFS `output/analysis/cluster_profile/` + bảng nhỏ `output/reports/cluster_profile.csv`.
- Thứ tự: **số liệu trước, diễn giải sau**; tách **Data Finding / Business Interpretation / Recommendation**.

| Cluster | Khách | Median R / F / M | Median Tenure | % doanh thu | Tên đề xuất (sau khi xem số liệu) |
|---|---:|---|---:|---:|---|
| 0 | 1,179 (20.2%) | 17 / 13 / 4,877 | 678 | **73.5%** | Khách giá trị cao, trung thành |
| 3 | 1,442 (24.7%) | 185 / 4 / 1,445 | 619 | 16.4% | Có nguy cơ rời bỏ |
| 2 | 1,261 (21.6%) | 24 / 3 / 715 | **220** | 6.4% | Khách mới / tiềm năng |
| 1 | 1,957 (33.5%) | 402 / 1 / 269 (69% mua 1 lần) | 440 | 3.7% | Đã rời bỏ / mua một lần |

- Phát hiện chính: **20% khách (Cluster 0) tạo 73.5% doanh thu**; cụm đông nhất (Cluster 1, 33.5%) chỉ 3.7%.
- Recommendation (giữ chân / win-back / nuôi dưỡng / kích hoạt chi phí thấp) là **đề xuất**, chưa có thực nghiệm chứng minh tăng doanh thu.

# Visualization

7 biểu đồ PNG (Matplotlib, `output/reports/charts/`), mỗi biểu đồ trả lời 1 câu hỏi: số khách theo cụm · Recency/Frequency/Monetary
trung bình (kèm median) · % khách vs % doanh thu · phân phối R/F/M theo cụm (boxplot, thang log) · K vs Silhouette + cụm nhỏ nhất.
Vẽ trên host từ bảng nhỏ do Spark export (host không đọc HDFS); pandas chỉ dùng cho các bảng này. Không có dashboard (đã bỏ khỏi roadmap).

# Big Data characteristics

Trả lời trung thực câu "dataset có phải Big Data không?":

| Đặc trưng | Project này |
|---|---|
| **Volume** | 1,067,371 dòng, CSV 94.3 MB — **không lớn**: một máy với pandas vẫn xử lý được. File nằm trong **1 block** HDFS (< 128 MB) |
| **Velocity** | Dữ liệu lịch sử, xử lý theo lô (batch) — không có streaming |
| **Variety** | 1 nguồn, dạng bảng (CSV từ Excel) |
| **Veracity** | Đây là phần "khó" thật: 22.77% thiếu CustomerID, 34,335 dòng trùng (gồm 22,523 do 2 sheet chồng lấn), hóa đơn hủy, mã phí, đơn mua ảo bị hủy → 9 rule làm sạch có kiểm chứng |

Khía cạnh Big Data của project nằm ở **kiến trúc và cách làm**, không ở kích thước dữ liệu:
- Lưu trữ phân tán (HDFS: NameNode/DataNode, block, replication) và xử lý phân tán (Spark: driver/executor, partition, shuffle).
- Code viết bằng Spark DataFrame/MLlib nên **không đổi** khi dữ liệu lớn hơn — chỉ cần thêm DataNode/worker.
- **Giới hạn thực nghiệm:** project **không** đo hiệu năng khi dữ liệu tăng (Phase scale-up/benchmark đã bỏ), nên không kết luận
  "Spark nhanh hơn pandas" hay thời gian xử lý ở 10× dữ liệu. Cluster chỉ có 1 DataNode, 1 worker (4 core, 2 GB) trên 1 máy.

# Limitations

1. **Quy mô:** dữ liệu 94 MB, cluster 1 máy, replication = 1 (không chịu lỗi), không benchmark scale-up.
2. **Dữ liệu:** bỏ 22.77% dòng thiếu CustomerID (khách vãng lai không được phân khúc); 12,244 dòng hủy không ghép được dòng mua
   → Monetary vài khách hơi cao (vd 15098); dữ liệu 2009–2011, có tính mùa vụ (đỉnh tháng 10–11).
3. **Đặc trưng:** chỉ R, F, M — không có sản phẩm, lợi nhuận, nhân khẩu học, kênh bán.
4. **Mô hình:** K-Means giả định cụm dạng cầu, nhạy outlier (đã giảm bằng log1p); 1 seed (42), chưa đo độ ổn định qua nhiều seed;
   Silhouette 0.5325 = cấu trúc vừa phải, các cụm có chồng lấn.
5. **Đánh giá:** không có nhãn thật (ground truth); tên cụm là diễn giải của người phân tích; recommendation **chưa được kiểm chứng**
   bằng thực nghiệm (A/B test).
6. **Vận hành:** chưa có lệnh chạy toàn pipeline một lần (13 bước chạy tuần tự); host Windows không đọc trực tiếp HDFS
   (driver phải chạy trong container).

# How to Run

Yêu cầu: Python 3.12, Java 17 hoặc 21 (máy dev dùng JDK 21), Docker Desktop.

```bash
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env

# Phase 1 - Dataset (local)
python scripts\download_dataset.py        # tải + chuyển CSV (bỏ qua nếu đã có; --force để tải lại)
python scripts\profile_dataset.py         # profile bằng PySpark local -> output/reports/dataset_profile.json

# Phase 2 - Cluster
docker compose up -d
python scripts\init_hdfs.py               # tạo thư mục HDFS
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/spark_hdfs_smoke.py

# Phase 3 - Ingestion
python scripts\upload_to_hdfs.py          # raw local -> HDFS (SKIP nếu đã có bản giống hệt; --force để ghi đè)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_hdfs_ingestion.py

# Phase 4 - Preprocessing
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/preprocessing/clean_transactions.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_processed_data.py

# Phase 5 - RFM
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/feature_engineering/build_rfm.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_rfm.py

# Phase 6+7 - Scaling + K-Means + Evaluation (image Spark đã có numpy; nếu image cũ: docker compose build)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/clustering/evaluate.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/clustering/kmeans.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_clustering.py

# Phase 8+10 - Cluster Analysis + Visualization
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/analysis/cluster_analysis.py
python -m src.analysis.charts             # host (.venv), cần matplotlib
```
