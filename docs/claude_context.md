# Claude Context — Trạng thái hiện tại của project

> **Đọc file này đầu tiên khi mở phiên chat mới.** Sau đó đọc `docs/pending_work.md` để biết Phase tiếp theo cần làm gì.
> Cập nhật lần cuối: **2026-10-01 — PROJECT HOÀN THÀNH** (All implementation phases completed) **+ Web Demo bổ sung** **+ UI redesign Web Demo**. **Công việc kết thúc tại đây theo yêu cầu người dùng** — không còn việc bắt buộc; phiên mới chỉ báo cáo trạng thái, không sửa code khi chưa được yêu cầu (presentation layer — xem mục "Web Demo" ngay trước mục 9).

---

## 1. Project trong 30 giây

- **Đề tài:** Phân khúc khách hàng từ dữ liệu giao dịch lớn. Sinh viên làm **một mình**.
- **Pipeline mục tiêu:**
  `Online Retail II → HDFS → Spark/PySpark → Preprocessing → RFM → Feature Scaling → K-Means → Silhouette → Cluster analysis → Business insights → Biểu đồ (Plotly/Matplotlib)`
  (không dashboard, không scale-up benchmark — đã bỏ 2026-10-01)
- **Root project:** `C:\Users\Quang\Desktop\BigData` (đóng vai `customer-segmentation-bigdata/`). **Chưa phải git repo.**
- **Đã có:** dataset thật trong `data/raw/`, cluster HDFS + Spark chạy bằng Docker Compose, **raw dataset trên HDFS**
  (`/data/customer-segmentation/raw/online_retail_II.csv`), Spark đọc trực tiếp từ HDFS (đã kiểm tra 7/7 PASS),
  **dữ liệu giao dịch đã làm sạch** (`/data/customer-segmentation/processed/`, Parquet, 770,563 dòng, 10/10 PASS),
  **RFM** của 5,839 khách (`/data/customer-segmentation/rfm/`, Parquet, 11/11 PASS),
  **K-Means K = 4** (log1p + StandardScaler, Silhouette 0.5325) → `/data/customer-segmentation/output/clustering/` (10/10 PASS),
  **profile 4 cụm** (HDFS `output/analysis/cluster_profile/` + `output/reports/cluster_profile.csv`) + diễn giải (`08_business_analysis.md`)
  + **7 biểu đồ PNG** (`output/reports/charts/`).
- **Phase 12:** chạy lại end-to-end 13 bước (~4.9 phút) — mọi check PASS, output tạo lại giống hệt (7 PNG giống từng byte).
- **Phase 13+14:** README viết lại (trang giới thiệu), docs hoàn thiện, `docs/14_viva.md` (33 câu Q/A, demo script, checklist);
  xóa file không dùng (`src/pipeline.py`, `scripts/generate_scaled_dataset.py`, `dashboard/`, `notebooks/`) theo lựa chọn người dùng.
- **Web Demo (bổ sung sau roadmap):** trang tĩnh trình bày kết quả, chỉ đọc output — `python scripts\run_web_demo.py` → http://127.0.0.1:8000/web/.
- **Không còn việc bắt buộc.** Việc tùy chọn: `pending_work.md` → "Remaining tasks (optional)".

## 2. Quy tắc làm việc (bắt buộc)

1. **Mỗi lần chỉ làm đúng MỘT Phase**, xong phải **DỪNG và CHỜ người dùng duyệt**. Kể cả khi tin nhắn chứa đặc tả nhiều Phase.
2. **Không bịa số liệu.** Mọi con số trong docs/báo cáo phải lấy từ lần chạy thật; không giả định hệ thống chạy nếu chưa test.
3. **Stack cố định:** Python 3.12, PySpark/Spark, Hadoop HDFS, Spark MLlib, Docker Compose, **Matplotlib** (biểu đồ PNG — người dùng chọn). Jupyter/Plotly/Streamlit đã **bỏ** khỏi requirements ở Phase 13+14,
   CSV (raw), Parquet (processed). Không dashboard (Streamlit đã bỏ khỏi roadmap). **Không tự thêm** Kafka, Flink, Redis, Airflow, Kubernetes, FastAPI, React, microservices, auth.
4. **Pipeline chính dùng Spark DataFrame.** Không dùng pandas để xử lý toàn bộ dataset
   (pandas chỉ dùng trong `download_dataset.py` để đổi Excel → CSV, và cho bảng nhỏ đã aggregate khi vẽ biểu đồ).
5. **Từ Phase 3 trở đi pipeline chính đọc từ HDFS** qua `hdfs_loader.read_raw_transactions(spark)`, không đọc file local.
6. Người dùng muốn **hiểu** project → mỗi Phase phải giải thích khái niệm, lý do chọn giải pháp, câu hỏi giảng viên có thể hỏi.
7. **Báo cáo bằng tiếng Việt.** Cuối mỗi Phase cập nhật: doc của Phase, `PROJECT_EXPLANATION.md`, `claude_context.md`, `pending_work.md` (+ README nếu có lệnh mới).
8. Trước mỗi Phase đọc `README.md`, `claude_context.md`, `pending_work.md`, code/config liên quan. Khi mở chat mới: báo cáo trạng thái,
   **không sửa code cho đến khi người dùng xác nhận**.

## 3. Checklist khi bắt đầu phiên mới

```bash
cd C:\Users\Quang\Desktop\BigData
docker compose up -d                       # Docker Desktop phải đang chạy
docker compose ps                          # namenode, datanode (healthy), spark-master, spark-worker (Up)
.venv\Scripts\python scripts\init_hdfs.py  # idempotent, xác nhận 5 thư mục HDFS
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/spark_hdfs_smoke.py   # phải in RESULT: PASS
.venv\Scripts\python scripts\upload_to_hdfs.py   # phải in SKIP (raw đã có trên HDFS, giống hệt local)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_hdfs_ingestion.py   # RESULT: PASS
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_processed_data.py   # RESULT: PASS (processed Parquet còn nguyên)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_rfm.py   # RESULT: PASS (RFM Parquet còn nguyên)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_clustering.py   # RESULT: PASS (output clustering còn nguyên)
```

Nếu Docker Desktop chưa chạy (lỗi `open //./pipe/dockerDesktopLinuxEngine`): mở `C:\Program Files\Docker\Docker\Docker Desktop.exe`, chờ `docker info` OK.

Demo: `.venv\Scripts\python scripts\run_web_demo.py` → http://127.0.0.1:8000/web/ (web chỉ đọc file, không cần Docker; Docker cần cho phần demo HDFS/Spark UI 9870/8080/4040).

Rồi đọc `docs/pending_work.md` → mục **Next**.

## 4. Roadmap & trạng thái

| Phase | Nội dung | Trạng thái |
|---|---|---|
| 0 | Project skeleton | ✅ Done |
| 1 | Dataset: download + profile bằng PySpark | ✅ Done |
| 2 | Docker + Hadoop HDFS + Spark | ✅ Done |
| 3 | HDFS Ingestion: Raw → HDFS → Spark DataFrame | ✅ Done |
| 4 | Preprocessing (PySpark → Parquet trên HDFS) | ✅ Done |
| 5 | RFM | ✅ Done |
| 6 + 7 | Scaling + K-Means + Evaluation (K = 2..6, Silhouette + cluster size, chọn K) | ✅ Done (K = 4) |
| 8 + 10 | Cluster analysis + Visualization (≥ 4 biểu đồ, không dashboard) | ✅ Done (7 biểu đồ) |
| ~~9~~ | ~~Scale-up + benchmark~~ — **BỎ** (ghi là giới hạn thực nghiệm) | ❌ |
| ~~11~~ | ~~Dashboard Streamlit~~ — **BỎ** | ❌ |
| 12 | Final end-to-end check (chỉ sửa lỗi cần thiết) | ✅ Done |
| 13 + 14 | Documentation + Viva (`docs/14_viva.md`, ≥ 20 câu) — Phase cuối | ✅ Done (33 câu) |
| — | **Web Demo** (bổ sung sau roadmap, không đánh số Phase) — presentation layer | ✅ Done |

Roadmap rút gọn theo yêu cầu người dùng 2026-10-01. Đặc tả đầy đủ (yêu cầu + ghi chú kỹ thuật + demo commands): `docs/pending_work.md`.

## 5. Môi trường (đã kiểm tra thật 2026-09-30)

| Thành phần | Giá trị |
|---|---|
| OS | Windows 11 Home |
| Shell dùng được | PowerShell, Git Bash |
| Python host | 3.12.3 (`py -3.12`), venv `.venv/` → `.venv\Scripts\python.exe` |
| Java host | JDK 21.0.12 (`JAVA_HOME=C:\Program Files\Java\jdk-21.0.12.1`) |
| HADOOP_HOME / winutils | **Không có** → Spark local đọc file được; **ghi** file local từ Windows có thể lỗi |
| PySpark host | 4.0.4 |
| Package host | pyspark 4.0.4, pandas 3.0.6, openpyxl 3.1.5, pyyaml 6.0.3, python-dotenv 1.2.3, plotly 7.1.0, streamlit 1.64.0, jupyter 1.1.1 (vẫn còn trong .venv nhưng đã bỏ khỏi requirements), **matplotlib 3.11.2** (Phase 8+10) |
| Docker | Docker Desktop 29.1.2, Compose v2.40.3, 16 CPU, 7.6 GB RAM cho Docker |
| Mạng tới UCI | Chậm (~0.1–0.5 MB/s), chunked, **không hỗ trợ HTTP Range** |

Máy còn container của **project khác** (techshopping-*, work-odoo-1, learn-*, mysql-db) — **không đụng tới**.

## 6. Cluster (docker-compose project `customer-segmentation`)

| Service | Image | Địa chỉ trong Docker network | Port ra host |
|---|---|---|---|
| `namenode` | `apache/hadoop:3.4.1` (CentOS 7, Java 8, user `hadoop`) | `hdfs://namenode:8020` | 9870 (UI) |
| `datanode` | `apache/hadoop:3.4.1` | `datanode:9866` | 9864 (UI) |
| `spark-master` | `customer-segmentation/spark:4.0.4` (build từ `docker/spark/`) | `spark://spark-master:7077` | 8080 (UI), 7077, 4040 (driver UI) |
| `spark-worker` | như trên | — | 8081 (UI) |

- **Image Spark:** `apache/spark:4.0.4-java21-python3` (Ubuntu 22.04, **Python 3.10.12**, Java 21, user `spark` uid 185)
  + `pyyaml`, `python-dotenv`, **`numpy` 2.2.6** (Phase 6, bắt buộc cho `pyspark.ml`) + `spark-defaults.conf`.
  Sửa Dockerfile → `docker compose build spark-master` + `docker compose up -d` (recreate master/worker; HDFS không ảnh hưởng).
- **Worker:** 4 core, 2 GB. **Executor:** 1 GB (`SPARK_EXECUTOR_MEMORY`, mặc định 1g). Đo thật: 1 executor × 4 core.
- **HDFS config** (`docker/hadoop/hadoop.env`): `fs.defaultFS=hdfs://namenode:8020`, `dfs.replication=1`,
  `dfs.blocksize=128 MB`, name dir `/data/dfs/name`, data dir `/data/dfs/data` (trong container, trên named volume).
- **`docker/spark/spark-defaults.conf`:** `spark.hadoop.dfs.replication 1`, `spark.sql.timestampType TIMESTAMP_NTZ`, `spark.sql.session.timeZone UTC`.
- **Volumes:** `customer-segmentation_namenode-data`, `customer-segmentation_datanode-data` → còn sau `docker compose down`
  (đã test). `docker compose down -v` sẽ **xóa sạch HDFS**.
- **Project mount:** `./` → `/opt/project` trong container Spark (working dir + `PYTHONPATH`).
- **Env trong container Spark:** `SPARK_MASTER_URL=spark://spark-master:7077`, `HDFS_NAMENODE_HOST=namenode`,
  `HDFS_NAMENODE_PORT=8020`, `PYSPARK_PYTHON=python3`.
- **Driver chạy BÊN TRONG container `spark-master`** qua `spark-submit`. Windows `.venv` **không** kết nối trực tiếp được HDFS
  (NameNode trả IP DataNode nội bộ 172.21.x.x).
- RAM đo được khi idle: namenode ~316 MB, datanode ~295 MB, master ~223 MB, worker ~211 MB.

### Trạng thái HDFS hiện tại

```text
/data/customer-segmentation/          owner spark:supergroup, 755
├── raw/
│   └── online_retail_II.csv   94,268,848 bytes, 1 block (blk_1073741829), replication 1, owner hadoop, rw-r--r-- (Phase 3; khôi phục ở Phase 12)
├── processed/      Parquet 4 file part-0000[0-3]-*.snappy.parquet, 11,033,643 bytes (du: 10.5 M), 770,563 dòng, owner spark (Phase 4)
├── rfm/            Parquet 1 file part-00000-*.snappy.parquet, 74,485 bytes, 5,839 dòng (Phase 5)
├── output/
│   ├── clustering/   Parquet 1 file 76,297 bytes, 5,839 dòng (CustomerID, Recency, Frequency, Monetary, Cluster) (Phase 6)
│   └── analysis/cluster_profile/   Parquet 1 file, 4 dòng (profile từng cụm) (Phase 8)
└── evaluation/
    └── k_selection/  Parquet 1 file 3,550 bytes, 10 dòng (2 variant × K 2..6) (Phase 7)
/tmp/phase2-smoke/online_retail_II_sample.csv   89,813 bytes, 1,000 dòng — file test của Phase 2 (giữ lại cho spark_hdfs_smoke.py)
```

## 7. Cấu trúc code & trạng thái từng file

| File | Trạng thái | Nội dung |
|---|---|---|
| `configs/config.yaml` | ✅ | `dataset.*` (URL, tên file), `paths.*` (local), `hdfs.*` (6 thư mục), `spark.*`, **`preprocessing.*`** (timestamp_format, valid_invoice_pattern, cancel_prefix, 25 non_product_stockcodes, output_files 4, report_filename), `rfm.*` (`customer_col: CustomerID`, `analysis_date: null`, `analysis_date_offset_days: 1`, `output_files: 1`, `report_filename`), `clustering.*` (features `[Recency, Frequency, Monetary]`, `log_transform: true`, k 2..6, seed 42, max_iter 100, tol 1e-4, **`selected_k: 4`**, output_files 1, evaluation_report, kmeans_report), **`analysis.*`** (profile_csv, profile_json, customers_csv, charts_dir) |
| `.env.example` | ✅ | `HDFS_NAMENODE_HOST/PORT`, `SPARK_MASTER_URL=local[*]`, memory (đã bỏ `STREAMLIT_PORT`). **Chưa tạo `.env`** (không bắt buộc) |
| `requirements.txt` | ✅ | pyspark==4.0.4, pyyaml, python-dotenv, openpyxl, pandas, matplotlib>=3.8 (bỏ jupyter, plotly, streamlit ở Phase 13+14) |
| `docker-compose.yml` | ✅ | 4 service, healthcheck namenode/datanode, 2 named volume |
| `docker/hadoop/hadoop.env` | ✅ | core-site / hdfs-site qua envtoconf |
| `docker/spark/Dockerfile`, `spark-defaults.conf` | ✅ | |
| `src/config/settings.py` | ✅ | `PROJECT_ROOT`, `load_config()`, `load_env()` (không ghi đè env có sẵn), `hdfs_uri(path)` |
| `src/utils/spark_session.py` | ✅ | `get_spark(app_name, master)`: master từ `SPARK_MASTER_URL` (mặc định `local[*]`), driver/executor memory, `TIMESTAMP_NTZ`, UTC, `PYSPARK_PYTHON=sys.executable` |
| `src/ingestion/csv_reader.py` | ✅ | `RAW_COLUMNS`, `raw_schema()`, `read_raw_csv(spark, path, with_corrupt_record)`: toàn bộ STRING, `escape='"'`, kiểm tra header |
| `src/ingestion/hdfs_loader.py` | ✅ | `path_exists`, `list_dir`, `block_locations`, `delete_path` (Hadoop FS API qua py4j), `raw_dataset_uri()`, `read_raw_transactions(spark, uri=None, with_corrupt_record=False)` = **điểm vào duy nhất của raw**, `read_parquet` |
| `scripts/download_dataset.py` | ✅ | zip → kiểm tra HTTP/zip → xlsx → CSV + metadata; `--force`, `--retries`; 4xx không retry |
| `scripts/profile_dataset.py` | ✅ | Profile toàn bộ bằng PySpark → `output/reports/dataset_profile.json`; `--path` (file:// hoặc hdfs://), `--metadata`, `--out`; có kiểm tra chồng lấn sheet |
| `scripts/init_hdfs.py` | ✅ | Chạy trên host; `docker compose exec namenode hdfs ...`; mkdir, chown spark + chmod 755 **chỉ cho thư mục** (không -R — sửa ở Phase 12); idempotent |
| `scripts/spark_hdfs_smoke.py` | ✅ | Chạy trong container; version, job phân tán, list HDFS, đọc CSV, ghi/đọc/xóa Parquet → `RESULT: PASS` |
| `scripts/upload_to_hdfs.py` | ✅ | Chạy trên host: kiểm tra local (tồn tại/không rỗng/header) → `docker compose cp` → `hdfs dfs -put -f` → xóa tmp (root) → so size + sha256 → fsck. Đã có bản giống hệt → SKIP; khác → lỗi trừ khi `--force` |
| `scripts/check_hdfs_ingestion.py` | ✅ | Chạy trong container: HDFS file/block → schema → count/partition → corrupt + null → 5 dòng đầu so CSV local; số kỳ vọng từ metadata.json + dataset_profile.json; ghi `output/reports/hdfs_ingestion_check.json` → `RESULT: PASS` |
| `src/preprocessing/clean_transactions.py` | ✅ | Chạy bằng spark-submit. `prepare()` đổi tên + try_cast + `_amount`; `remove_cancellations()` (bỏ dòng hủy + dòng mua khớp 1-1 qua Window); `clean_transactions(raw, cfg)` → (df, report) 9 rule; `summarize()`; ghi Parquet `coalesce(4)` + `output/reports/preprocessing_report.json`. Hằng `RENAME`, `OUTPUT_COLUMNS` |
| `scripts/check_processed_data.py` | ✅ | Đọc lại Parquet processed → 10 check (schema, count, accounting, null, dup, invalid, Amount) → `output/reports/processed_check.json` → `RESULT: PASS` |
| `src/feature_engineering/build_rfm.py` | ✅ | Chạy bằng spark-submit. `resolve_analysis_date(df, rfm_cfg)`, `build_rfm(df, analysis_date, customer_col)` (groupBy + datediff/countDistinct/sum), `describe_rfm()` (min/percentile/mean/std/skew, IQR outlier, corr, top, mẫu); ghi Parquet `coalesce(1)` + `output/reports/rfm_report.json`. Hằng `FEATURES = [Recency, Frequency, Monetary]` |
| `scripts/check_rfm.py` | ✅ | Đọc lại RFM → 11 check, gồm **tính lại bằng Spark SQL và so từng khách** → `output/reports/rfm_check.json` → `RESULT: PASS` |
| `src/clustering/kmeans.py` | ✅ | `scale_features(rfm, cols, log_transform)` → (df + cột `features`, PipelineModel log1p→VectorAssembler→StandardScaler); `scaling_summary()`; `train_kmeans(df, k, seed, max_iter, tol)` (predictionCol `Cluster`); `cluster_rfm_means()`; `main` = model cuối với `selected_k` → HDFS `output/clustering` + `output/reports/kmeans_report.json` |
| `src/clustering/evaluate.py` | ✅ | `silhouette_score(pred)` (ClusteringEvaluator, squaredEuclidean); `evaluate_k_range(...)` (silhouette, sizes, WSSSE, numIter, time, TB R/F/M); `main` = 2 variant × K 2..6 → `output/evaluation/k_evaluation.{json,csv}` + HDFS `evaluation/k_selection` |
| `scripts/check_clustering.py` | ✅ | 10 check: schema, count, unique, null, đủ K cụm, sizes = report = evaluate, R/F/M = RFM, Silhouette tính lại = 0.5325, hội tụ → `output/reports/clustering_check.json` |
| `src/analysis/cluster_analysis.py` | ✅ | Spark (container): `add_tenure()` (Tenure = datediff(AnalysisDate, ngày mua đầu) từ processed), `profile_clusters()` → (profile DF: count, %, avg/median R/F/M/Tenure, TotalMonetary, MonetaryPct, OneTimeBuyerPct, *VsOverall; dict overall); ghi HDFS `output/analysis/cluster_profile` + `output/reports/cluster_profile.{csv,json}` + `output/clustering/customer_clusters.csv`; 7 check → `RESULT: PASS` |
| `src/analysis/charts.py` | ✅ | Host (.venv, `python -m src.analysis.charts`): Matplotlib (Agg) đọc 3 bảng nhỏ → 7 PNG 150 dpi trong `output/reports/charts/`; `CHARTS` = file → câu hỏi phân tích |
| ~~`src/pipeline.py`, `scripts/generate_scaled_dataset.py`, `dashboard/`, `notebooks/`~~ | 🗑 **đã xóa** (Phase 13+14, người dùng chọn) | stub/placeholder không dùng |

### Docs

| File | Trạng thái |
|---|---|
| `docs/01_problem_definition.md` | ✅ (Input + **Result** số liệu thật; bỏ dashboard khỏi mục tiêu) |
| `docs/02_dataset.md` | ✅ số liệu thật Phase 1 |
| `docs/04_architecture.md` | ✅ Phase 2 + **Phase 3 (mục 7)** + **Phase 12 (mục 9: luồng end-to-end, 13 bước + thời gian, bằng chứng tái lập, lỗi init_hdfs)** |
| `docs/03_preprocessing.md` | ✅ Phase 4 (schema, Amount, 9 rule với số thật, hủy + ghép 1-1, danh sách mã, quyết định, test, hạn chế) |
| `docs/05_rfm.md` | ✅ Phase 5 (transaction→customer level, RFM, AnalysisDate, định nghĩa, thống kê, phân bố, outlier/skew, test, hạn chế) |
| `docs/06_kmeans.md` | ✅ Phase 6 (pipeline, vì sao scale, log1p, mean/std trước/sau, centroid/distance/assignment/update/iteration/convergence, kết quả K = 4, test) |
| `docs/07_evaluation.md` | ✅ Phase 7 (metric + lý do, bảng K = 2..6 × 2 variant, TB R/F/M, tiêu chí, chọn K = 4, hạn chế metric). Đổi tên từ `08_evaluation.md`; **đã xóa** placeholder `07_experiment.md` (Phase 9 bỏ) |
| `docs/08_business_analysis.md` | ✅ Phase 8+10 (profile mean + median, so sánh vs overall, Data Finding / Business Interpretation / Recommendation từng cụm, tên đề xuất, 7 biểu đồ + câu hỏi + quan sát, hạn chế) |
| `docs/PROJECT_EXPLANATION.md` | ✅ Hoàn chỉnh: Overview (+ mục lục), Architecture, Dataset, HDFS, Spark, Preprocessing, RFM, Feature Scaling, K-Means, Evaluation, Cluster Analysis, Visualization, **Big Data characteristics**, **Limitations**, How to Run |
| `docs/14_viva.md` | ✅ Phase 13+14: tóm tắt 1 phút, cheat sheet, 33 câu Q/A/Why (Big Data, HDFS, Spark, preprocessing, RFM, scaling, K-Means, Silhouette, cluster, business, hạn chế), demo script 7 phút, final checklist |
| `docs/images/` | ✅ 3 PNG (05, 06, 07) chép từ `output/reports/charts/` cho README (output/ không commit) |
| `web/` + `scripts/build_web_summary.py`, `scripts/run_web_demo.py` | ✅ Web Demo (xem mục "Web Demo" ở trên) |
| `README.md` | ✅ Viết lại ở Phase 13+14: trang giới thiệu (bài toán, kết quả, ảnh, kiến trúc, công nghệ, cài đặt, 13 bước chạy, cấu trúc, tài liệu, hạn chế, troubleshooting) |

### Quy ước code

- Docstring/comment **tiếng Việt**, tên hàm/biến tiếng Anh.
- Script trong `scripts/` tự thêm project root vào `sys.path` (`sys.path.insert(0, parents[1])`) rồi `from src...`.
- **Không hard-code** đường dẫn/tham số: đọc `configs/config.yaml` qua `load_config()`, URI HDFS qua `hdfs_uri()`.
- Script trả exit code (0 = OK, 1 = lỗi) và in log rõ ràng.
- SparkSession luôn tạo qua `get_spark()`.

## 8. Dữ liệu — số liệu thật (chi tiết: `docs/02_dataset.md`)

| Chỉ số | Giá trị |
|---|---|
| File | `data/raw/online_retail_II.csv` — 94,268,848 bytes; kèm `.zip` (45,622,418 B, sha256 `572e3627…08e67bfb`), `.xlsx`, `.metadata.json` |
| Records × columns | **1,067,371 × 8** (sheet 2009-2010: 525,461; sheet 2010-2011: 541,910) |
| Cột raw | `Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country` |
| Tương ứng tên "chuẩn" (Online Retail I) | Invoice→**InvoiceNo**, Price→**UnitPrice**, Customer ID→**CustomerID** |
| InvoiceDate | 2009-12-01 07:45:00 → **2011-12-09 12:50:00** (25 tháng) |
| Distinct | 5,942 Customer ID · 53,628 Invoice · 43 Country · 5,305 StockCode · 5,698 Description |
| Missing | **Customer ID 243,007 (22.77%)** trong 8,752 hóa đơn · Description 4,382 (0.41%) · cột khác 0 |
| Duplicate (8 cột) | **34,335 dòng thừa (3.22%)** = 22,523 do **2 sheet chồng lấn** (2010-12-01 08:26 → 2010-12-09 20:01, giống hệt 100%) + 11,812 trùng khác |
| Hóa đơn hủy `C…` | 19,494 dòng / 8,292 hóa đơn (19,493 Quantity < 0; 1 dòng C496350 `M` Quantity = 1) |
| Hóa đơn `A…` | 6 dòng "Adjust bad debt" (5 Price âm + 1 Price +11,062.06), không có Customer ID |
| Quantity | min −80,995 · p50 3 · p99 100 · max 80,995 · mean 9.94. Quantity < 0: 22,950 (3,457 không phải hủy, **tất cả** không có Customer ID); = 0: 0 |
| Price | min −53,594.36 · p50 2.10 · p99 18.00 · max 38,970.00 · mean 4.65. Price = 0: 6,202 (71 có Customer ID); < 0: 5 |
| StockCode không phải sản phẩm | 63 mã, 6,094 dòng (heuristic không khớp `^\d{5}[A-Za-z]*$`): POST 2,122, DOT 1,446, M 1,421, C2 282, D 177, S 104, BANK CHARGES 102, ADJUST 67, AMAZONFEE 43… (vài mã như `DCGS0058` có thể là sản phẩm thật) |
| Khác | Country "Unspecified" 756 dòng · 13 khách có >1 Country · UK 91.94% số dòng · Description có khoảng trắng thừa · 0 lỗi parse kiểu · 0 corrupt record |

### Số liệu Phase 3 (raw trên HDFS — dùng thống nhất cho các Phase sau)

| Chỉ số | Giá trị (đo thật 2026-10-01) |
|---|---|
| HDFS URI | `hdfs://namenode:8020/data/customer-segmentation/raw/online_retail_II.csv` (= `hdfs_loader.raw_dataset_uri()`) |
| Size | 94,268,848 bytes (khớp local) · sha256 `b0dab2a011f1f8f35dcaf4ab3f9b93484c8b6a61614267157e648f3d83138e8c` (CSV, khác sha256 của .zip) |
| Block | 1 block 94,268,848 B (block size 128 MB), replication 1, DataNode `datanode` (172.21.0.3:9866), HEALTHY |
| Spark đọc | 1,067,371 dòng × 8 cột STRING, **4 partition** (split ≈ 23.5 MB, defaultParallelism 4), 0 corrupt record |
| Null | Customer ID 243,007 · Description 4,382 · cột khác 0 (khớp Phase 1) |
| Báo cáo | `output/reports/hdfs_ingestion_check.json` |

### Số liệu Phase 4 (processed — dùng thống nhất cho Phase 5+; chi tiết `docs/03_preprocessing.md`)

| Chỉ số | Giá trị (đo thật 2026-10-01) |
|---|---|
| HDFS URI | `hdfs://namenode:8020/data/customer-segmentation/processed/` (Parquet, 4 file, 4 partition khi đọc) |
| Schema | `InvoiceNo` string · `StockCode` string · `Description` string · `Quantity` int · `InvoiceDate` timestamp_ntz · `UnitPrice` double · `CustomerID` string · `Country` string · `Amount` double |
| Dòng | 1,067,371 → **770,563** (72.19%; removed 296,808) |
| Removed theo rule | date 0 · values 0 · duplicate 34,335 · cancelled 25,250 (19,104 hủy + 6,146 mua khớp) · A-invoice 6 · Qty≤0 3,393 · Price≤0 2,621 · missing CustomerID 228,487 · non-product 2,716 |
| Khách / hóa đơn / mã / nước | **5,839** / 36,338 / 4,613 / 41 |
| InvoiceDate | 2009-12-01 07:45:00 → **2011-12-09 12:50:00** (max dùng cho AnalysisDate ở Phase 5) |
| Quantity / UnitPrice / Amount | 1–19,152 / 0.001–649.50 / 0.001–38,970.00; tổng Amount **16,521,624.51** |
| NULL / trùng / invalid | 0 / 0 / 0 |
| Outlier đã biết | Khách 15098: dòng 60 × 649.50 = 38,970.00 bị hủy qua mã `M` (không khớp được) → vẫn còn |
| Báo cáo | `output/reports/preprocessing_report.json`, `output/reports/processed_check.json` |

### Số liệu Phase 5 (RFM — dùng thống nhất cho Phase 6+; chi tiết `docs/05_rfm.md`)

| Chỉ số | Giá trị (đo thật 2026-10-01) |
|---|---|
| HDFS URI | `hdfs://namenode:8020/data/customer-segmentation/rfm/` (Parquet 1 file) |
| Schema | `CustomerID` string · `Recency` int · `Frequency` bigint · `Monetary` double (giá trị gốc) |
| AnalysisDate | **2011-12-10** (max InvoiceDate processed 2011-12-09 12:50:00 + 1 ngày) |
| Khách | **5,839** (= CustomerID distinct trong processed) |
| Recency | min 1 · p25 26 · median 96 · p75 380 · p90 534 · p99 727 · max 739 · mean 200.91 · std 208.61 · skew 0.89 |
| Frequency | min 1 · p25 1 · median 3 · p75 7 · p90 13 · p99 46 · max 369 · mean 6.22 · std 12.64 · skew **11.96** |
| Monetary | min 2.90 · p25 336.09 · median 851.01 · p75 2,207.08 · p90 5,321.48 · p99 26,924.01 · max 579,128.64 · mean 2,829.53 · std 13,900.43 · skew **26.61** |
| Skew sau log1p (thử nhanh) | Recency −0.45 · Frequency 1.01 · Monetary 0.24 |
| Outlier IQR (chỉ đếm) | Frequency > 16: 421 · Monetary > 5,013.57: 616 · Recency: 0 |
| Khác | Frequency = 1: 1,613 khách (27.6%) · top 1% (58 khách) = 31.02% Monetary · 13 khách Monetary > 100k · corr R–F −0.26, R–M −0.125, F–M 0.625 |
| Tổng | Σ Frequency 36,338 (= số hóa đơn) · Σ Monetary 16,521,624.49 |
| Khách đáng chú ý | 18102 (M max 579,128.64) · 14911 (F max 369) · 16446 (M min 2.90 — đơn 80,995 sp đã bị hủy) · 15098 (M 39,619.50 gồm dòng hủy qua `M` không ghép được) |
| Báo cáo | `output/reports/rfm_report.json`, `output/reports/rfm_check.json` |

### Số liệu Phase 6+7 (clustering — dùng thống nhất cho Phase 8+; chi tiết `docs/06_kmeans.md`, `docs/07_evaluation.md`)

| Chỉ số | Giá trị (đo thật 2026-10-01) |
|---|---|
| HDFS URI | `hdfs://namenode:8020/data/customer-segmentation/output/clustering/` (Parquet 1 file) |
| Schema | `CustomerID` string · `Recency` int · `Frequency` bigint · `Monetary` double · `Cluster` int (R/F/M gốc) |
| Scaling | log1p → StandardScaler; log mean/std: R 4.4699/1.5292 · F 1.5447/0.8060 · M 6.7952/1.3788; sau scale mean 0 std 1 |
| Silhouette K 2..6 (log1p) | 0.6266 · 0.5060 · **0.5325** · 0.5145 · 0.4944 (cụm nhỏ nhất 39.80% · 21.27% · 20.19% · 7.74% · 7.72%) |
| Silhouette K 2..6 (không log) | 0.4146 · 0.6780 · 0.5549 · 0.7542 · 0.7728 (cụm nhỏ nhất 2–21 khách ở K ≥ 3) |
| Model cuối | K = 4, seed 42, k-means||, numIter 27/100, WSSSE 4,859.15, Silhouette 0.5325 |
| Cụm (chưa đặt tên) | 0: 1,179 khách (TB R 28.33 / F 19.12 / M 10,296.27) · 1: 1,957 (395.06 / 1.38 / 313.11) · 2: 1,261 (29.03 / 3.03 / 837.64) · 3: 1,442 (228.81 / 5.05 / 1,881.63) |
| Centroid (z) | 0: (−1.08, 1.51, 1.34) · 1: (0.88, −0.88, −0.94) · 2: (−0.89, −0.29, −0.22) · 3: (0.46, 0.21, 0.38) |
| Báo cáo | `output/evaluation/k_evaluation.{json,csv}`, `output/reports/kmeans_report.json`, `output/reports/clustering_check.json` |

### Số liệu Phase 8+10 (profile cụm — chi tiết `docs/08_business_analysis.md`)

| Cluster | Khách (%) | Avg R / F / M | Median R / F / M | Median Tenure | % mua 1 lần | % doanh thu | Tên đề xuất |
|---|---|---|---|---:|---:|---:|---|
| 0 | 1,179 (20.19%) | 28.33 / 19.12 / 10,296.27 | 17 / 13 / 4,877.30 | 678 | 0.00% | **73.48%** | Giá trị cao, trung thành |
| 1 | 1,957 (33.52%) | 395.06 / 1.38 / 313.11 | 402 / 1 / 269.09 | 440 | 69.09% | 3.71% | Đã rời bỏ / mua 1 lần |
| 2 | 1,261 (21.60%) | 29.03 / 3.03 / 837.64 | 24 / 3 / 714.96 | **220** | 18.72% | 6.39% | Khách mới / tiềm năng |
| 3 | 1,442 (24.70%) | 228.81 / 5.05 / 1,881.63 | 185 / 4 / 1,444.58 | 619 | 1.73% | 16.42% | Có nguy cơ rời bỏ |
| Tất cả | 5,839 | 200.91 / 6.22 / 2,829.53 | 96 / 3 / 851.01 | 530 | 27.62% | 16,521,624.49 | |

Khách đáng chú ý: 18102, 14911 → Cluster 0; 12346, 16446 → Cluster 1; 15098 → Cluster 3.
Biểu đồ: `output/reports/charts/01_customer_count_by_cluster.png` … `07_k_vs_silhouette.png`.

### Phase 12 — end-to-end (chi tiết `docs/04_architecture.md` mục 9)

13 bước tuần tự, 13/13 exit 0, tổng ≈ 294 s (gồm khởi động spark-submit): init_hdfs 16.8 s · smoke 14.6 · upload (SKIP) 10.5 ·
check_ingestion 13.2 · clean 58.0 · check_processed 21.3 · build_rfm 41.5 · check_rfm 17.8 · evaluate 42.9 · kmeans 21.4 ·
check_clustering 15.8 · cluster_analysis 15.6 · charts 4.7. 20/20 file `output/` tạo lại (sau 07:13:09 UTC) và giống hệt bản trước
(7 PNG giống từng byte); HDFS output là file mới (UUID mới). Lỗi sửa: `init_hdfs.py` -R đổi owner file raw.

### Web Demo (phần bổ sung sau roadmap — presentation layer, 2026-10-01)

- Thêm **sau khi hoàn thành roadmap chính**, **không đánh số Phase**. Chỉ **đọc** output đã có; không sửa/không gọi HDFS, Spark,
  preprocessing, RFM, K-Means; không chạy lại mô hình; không database, không auth, không Docker riêng, không Streamlit.
- Công nghệ: HTML + CSS + Vanilla JS (không framework/CDN, chạy offline) + server tĩnh `http.server` (thư viện chuẩn) chỉ bind `127.0.0.1`.
  Giao diện **tiếng Việt**; tên field/file giữ tiếng Anh. Số hiển thị định dạng en-US (1,067,371 · 0.5325) cho khớp docs/terminal.
- File: `scripts/build_web_summary.py` (output JSON + PNG → `output/reports/summary.json`, 7 check nhất quán),
  `scripts/run_web_demo.py` (build summary + server; chỉ cho `/web/`, `/output/reports/`; còn lại 404; `--port`),
  `web/index.html`, `web/style.css`, `web/app.js`, `web/cluster_interpretation.json` (chữ diễn giải từ `docs/08`, không chứa số;
  chỉ hiển thị khi khớp K = 4 và id cụm).
- Chạy: `.venv\Scripts\python scripts\run_web_demo.py` → http://127.0.0.1:8000/web/ (port bận → `--port 8001`).
- Section: Tổng quan dữ liệu (6 KPI + 9 rule) · Pipeline (8 bước + số) · RFM (3 thẻ + bảng thống kê) · Đánh giá K-Means (SVG Silhouette
  theo K, 2 variant, K được chọn; kích thước nhóm; bảng K) · Nhóm khách hàng (bảng + 4 thẻ: đặc điểm / diễn giải / đề xuất) ·
  Trực quan hóa (7 PNG có sẵn) · Kiến trúc. Mục "Hạ tầng demo" (nút 9870/8080/4040 + kiểm tra trạng thái) **đã xóa theo yêu cầu
  người dùng**; giao diện phóng to để trình chiếu (font gốc 19px, nội dung rộng tối đa 1560px).
- URL hạ tầng (mở trực tiếp trong trình duyệt khi demo, không có trên web): HDFS http://localhost:9870 · Spark Master http://localhost:8080 ·
  Spark Application http://localhost:4040 (chỉ khi job đang chạy).
- Kiểm tra thật: mọi route 200 (data/, configs/ → 404); **62/62 con số trong DOM đã render khớp output gốc** (Edge headless `--dump-dom`);
  không ảnh hỏng; `output/` so với bản sao lưu: giống hệt, file mới duy nhất `summary.json`; bố cục 1600px và 520px không tràn.
  Sau khi xóa mục hạ tầng + phóng to: kiểm tra lại 62/62 số vẫn khớp.

- **UI redesign cho Web Demo sau khi pipeline Big Data đã hoàn thành (2026-10-01, theo yêu cầu người dùng):** đổi phong cách từ
  "dashboard" sang **hệ thống phân tích dữ liệu nội bộ / báo cáo BI**. Chỉ sửa `web/index.html`, `web/style.css`, `web/app.js`;
  **không đổi** dữ liệu, `summary.json`, scripts, pipeline, kết quả, roadmap kỹ thuật.
  - Bỏ: hero section, chip công nghệ, thẻ KPI tô màu, thẻ có viền màu, nhiều màu nhấn, bo góc lớn, khối nền tối.
  - Mới: header mỏng + **sidebar** mục lục (scrollspy, active state); palette `#F5F6F7` / `#FFFFFF` / `#20242A` / `#68707A` / `#D9DDE2`,
    **1 màu nhấn `#245B73`**, bo góc 3px, không gradient/shadow/glow; font Inter → IBM Plex Sans → Segoe UI/system (offline).
  - Mục: Tổng quan (dải tóm tắt 5 số + bảng thông tin bộ dữ liệu) · Dữ liệu (bảng 9 quy tắc) · Phân tích RFM (bảng chỉ số – ý nghĩa – giá trị
    + cách tính) · Phân nhóm khách hàng (2 biểu đồ SVG cột: phân bố khách, % khách vs % doanh thu; bảng nhóm + dòng toàn bộ; "Nhận xét") ·
    Đánh giá mô hình (biểu đồ SVG Silhouette theo K; bảng K; mô hình được chọn; số khách mỗi nhóm) · Trực quan hóa (Hình 1–7 PNG) ·
    Kiến trúc hệ thống (sơ đồ khối có viền, mũi tên tĩnh; bảng container). Mục "Môi trường thực nghiệm" (bảng 9870 / 8080 / 4040)
    đã thêm rồi **xóa theo yêu cầu người dùng** — trang kết thúc ở "Kiến trúc hệ thống"; khi demo mở trực tiếp các URL trong tab mới.
  - Animation: chỉ transition 150 ms (hover, sidebar) + biểu đồ SVG hiện dần 200 ms khi tải.
  - Kiểm tra: 62/62 số khớp output gốc; route 200; không ảnh hỏng; `output/` không đổi; không còn gradient/shadow/glow/chữ "AI";
    bố cục 1600px và 760px (sidebar chuyển thành thanh ngang) không tràn.

## 9. Quyết định kỹ thuật đã chốt

| # | Quyết định | Lý do |
|---|---|---|
| 1 | PySpark/Spark **4.0.4** (không phải 3.5) | JDK 21 trên máy; Spark 4 hỗ trợ chính thức Java 17/21; client = cluster version |
| 2 | Hadoop **3.4.1** | Trùng Hadoop client trong Spark 4.0.4 |
| 3 | Excel → **CSV** ngay khi download, giữ `.zip`/`.xlsx` | Spark đọc CSV native, song song; không sửa giá trị |
| 4 | Raw giữ **tên cột gốc**, đọc toàn bộ **STRING** | Tầng raw không mất thông tin; ép kiểu bằng `try_cast` ở preprocessing |
| 5 | **`TIMESTAMP_NTZ`** + session tz UTC | InvoiceDate không có timezone; TIMESTAMP mặc định làm lệch +7h khi `collect()` |
| 6 | Driver chạy **trong container** `spark-master` | Host không truy cập được DataNode trong Docker network |
| 7 | Python cluster **3.10.12** (image chính thức) | Driver + executor cùng image → không lệch. Muốn 3.12 phải build image riêng (chưa làm) |
| 8 | `dfs.replication=1` (server + client) | Chỉ 1 DataNode. Hạn chế: không chịu lỗi |
| 9 | Spark Standalone, không YARN | Đủ cho 1 máy, ít thành phần |
| 10 | Thư mục HDFS owner `spark` (chỉ thư mục); file raw upload bằng `hadoop` (superuser), `rw-r--r--` | Spark chạy bằng user `spark` cần quyền ghi thư mục; raw chỉ đọc |
| 11 | Download: retry 3 lần tải lại từ đầu; HTTP 4xx không retry | Server UCI hay cắt kết nối, không hỗ trợ Range |
| 12 | Upload raw: `docker compose cp` + `hdfs dfs -put` (chạy từ host) | hdfs CLI chỉ có trong container; host không tới được DataNode |
| 13 | Không upload trùng: so **size + sha256**; khác → lỗi, chỉ ghi đè khi `--force` | Không âm thầm thay raw; idempotent |
| 14 | Raw trên HDFS owner `hadoop`, `rw-r--r--`; pipeline chỉ đọc `raw/` | Giữ nguyên raw dataset |
| 15 | Giữ `/tmp/phase2-smoke/` trên HDFS | `spark_hdfs_smoke.py` dùng mặc định file này |
| 16 | Số kỳ vọng của check lấy từ metadata.json + dataset_profile.json, không hard-code | Một nguồn sự thật từ Phase 1 |
| 17 | Processed đổi tên `InvoiceNo`, `UnitPrice`, `CustomerID`; `CustomerID` kiểu **string**; `InvoiceDate` timestamp_ntz; `Amount = round(Quantity × UnitPrice, 3)` | Tên chuẩn; ID không phải số; round chỉ bỏ sai số float |
| 18 | Loại **mọi** dòng trùng 8 cột (34,335 = 22,523 overlap + 11,812) | Không phân biệt được với lỗi ghi trùng |
| 19 | Hóa đơn hủy: bỏ dòng hủy **+ dòng mua khớp 1-1** (CustomerID, StockCode, \|Quantity\|, UnitPrice, mua ≤ lần hủy cuối) — **người dùng chọn** | Bỏ giao dịch mua ảo (80,995 / 74,215 sp bị hủy sau 12–16 phút) |
| 20 | Mã không phải sản phẩm: **danh sách tường minh** 25 mã trong config, không regex — **người dùng chọn** loại bỏ | Regex bắt nhầm SP1002, DCGS*, PADS, `47503J ` |
| 21 | Không trim Description; Country giữ nguyên | Không dùng cho RFM |
| 22 | Processed ghi thẳng vào `hdfs.processed_dir`, `mode("overwrite")`, `coalesce(4)` | Chạy lại được; tránh 200 file nhỏ |
| 23 | **AnalysisDate = to_date(max InvoiceDate processed) + 1 ngày = 2011-12-10** (config `rfm.analysis_date: null`) | Theo dữ liệu, không hard-code; khách mua ngày cuối có Recency 1 |
| 24 | Recency theo **ngày** (`datediff` trên `to_date`), int | Giờ trong ngày không mang ý nghĩa hành vi |
| 25 | Frequency = `countDistinct(InvoiceNo)` (bigint); Monetary = `round(sum(Amount), 2)` | Số lần mua thực tế, không phải số dòng |
| 26 | RFM lưu giá trị **gốc** (chưa scale), giữ mọi outlier (chỉ đếm theo IQR) | Outlier là khách lớn thật; scaling thuộc Phase 6 |
| 27 | **log1p + StandardScaler(withMean, withStd)** | Không log1p → K = 3..6 đều có cụm 2–21 khách (outlier, z tới 28.7/41.5) |
| 28 | KMeans k-means||, seed 42, maxIter 100, tol 1e-4 | Tái lập; đủ vòng để hội tụ (K = 4: 27 vòng) |
| 29 | **K = 4** (`clustering.selected_k`) | Silhouette cao nhất trong K = 3..6 (0.5325), cụm nhỏ nhất 20.19%, 4 cụm khác rõ trên R/F/M; K = 2 (0.6266) quá thô |
| 30 | Chọn K 2 bước: `evaluate.py` (mọi K) → điền `selected_k` → `kmeans.py` (model cuối) | "Không tự chọn K trước"; đổi K chỉ cần sửa config |
| 31 | Output clustering chỉ cho K đã chọn, lưu R/F/M gốc + Cluster; metric mọi K ở `k_evaluation.*` + HDFS `evaluation/k_selection` | Đơn giản; bảng nhỏ local để vẽ biểu đồ |
| 32 | Thêm numpy vào image Spark | `pyspark.ml` cần numpy |
| 33 | Biểu đồ bằng **Matplotlib → PNG** (người dùng chọn), vẽ trên host từ bảng nhỏ Spark export | Chèn thẳng vào báo cáo; host không đọc HDFS |
| 34 | Profile gồm cả **median** (+ mean theo đề bài) và **Tenure** (chỉ mô tả, không dùng phân cụm) | R/F/M lệch → median đại diện hơn; Tenure phân biệt khách mới/lâu năm |
| 35 | Tên cụm chỉ có trong docs (đề xuất sau khi xem số liệu), code/biểu đồ giữ "Cluster i" | Không gán nhãn tự động |
| 36 | Phase 12 không viết `src/pipeline.py`; chạy 13 bước tuần tự (bảng ở `04_architecture.md` mục 9) | Roadmap mới: chỉ kiểm tra, không refactor |
| 37 | Phase 13+14: xóa mọi stub/placeholder không dùng + bỏ jupyter/plotly/streamlit khỏi requirements (người dùng chọn) | Project gọn, không còn "Triển khai ở Phase…" |
| 38 | Web Demo = static HTML/CSS/JS + `http.server` 127.0.0.1, đọc `output/reports/summary.json` (gom từ output có sẵn) | Không backend/DB/framework; không ảnh hưởng pipeline |
| 39 | Diễn giải cụm cho web ở `web/cluster_interpretation.json` (chữ, không số), chỉ hiện khi khớp K và id cụm | Không hard-code số; đổi K thì web tự ẩn tên nhóm |

## 10. Bẫy kỹ thuật đã gặp (đừng lặp lại)

1. `http.client.IncompleteRead` **không phải** `OSError` → bắt `http.client.HTTPException`.
2. Spark CSV mặc định escape bằng `\`; CSV của pandas dùng `""` → `.option("escape", '"')` (đã có trong `csv_reader.py`).
3. Spark 4 bật **ANSI mode**: `cast` giá trị lỗi sẽ throw → dùng `try_cast`, `try_to_timestamp`.
4. `TIMESTAMP` có timezone + `collect()` → lệch theo giờ máy (UTC+7) → dùng `TIMESTAMP_NTZ`.
5. **Git Bash** đổi `/tmp/...` thành `C:/.../Temp/...` khi truyền cho `docker.exe` → `MSYS_NO_PATHCONV=1`. PowerShell và `subprocess` từ Python không bị.
6. Spark bỏ qua file/thư mục tên bắt đầu `_` hoặc `.` (coi là ẩn, như `_SUCCESS`) → không đặt tên output kiểu `_xxx`.
7. Ngay sau khi tạo SparkSession, `getExecutorMemoryStatus`/`defaultParallelism` có thể báo 0 executor → đo sau khi chạy job.
8. Cột `Customer ID` có dấu cách → dùng backtick trong biểu thức: ``F.col("`Customer ID`")``, hoặc đổi tên ở Phase 4.
9. Console Windows hiển thị `£` thành `�`; file CSV vẫn đúng UTF-8.
10. `hdfs dfs -cat ... | head` in `cat: Unable to write to output stream` — vô hại (pipe bị đóng).
11. Khi truyền `--driver-memory` phải đặt ở `spark-submit`; `spark.driver.memory` trong builder chỉ có tác dụng khi Python tự khởi động JVM (local mode).
12. `docker compose cp` tạo file trong container với owner **root** → xóa file tạm bằng `docker compose exec -u root namenode rm -f ...` (user hadoop không xóa được trong /tmp sticky).
13. Spark không cho query **chỉ** cột `_corrupt_record` trên CSV chưa cache → `df.cache()` trước khi `filter(_corrupt_record IS NOT NULL).count()`.
14. `hdfs dfs -checksum` là MD5-of-CRC32 theo block, **không** so được với sha256 local → so bằng `hdfs dfs -cat <file> | sha256sum` trong container.
15. Docker Desktop có thể tắt sau khi khởi động lại máy → lỗi `dockerDesktopLinuxEngine` → mở Docker Desktop trước.
16. Sau shuffle (dropDuplicates, Window, groupBy) DataFrame cache có **200 partition** (`spark.sql.shuffle.partitions`) → ghi ra 200 file Parquet nhỏ → `coalesce(n)` trước khi write.
17. `df.filter(~cond)` **bỏ luôn dòng có cond = NULL** (NOT NULL = NULL) → bọc `F.coalesce(cond, F.lit(False))` để không xóa âm thầm.
18. Phần lớn rule bị "ăn" bởi rule trước (vd Quantity < 0 chủ yếu là dòng hủy) → báo cáo cả `detected_in_raw` lẫn `records_removed`.
19. PowerShell pipe nội dung file (`Get-Content | python`) thêm BOM → `json.load` lỗi; đọc JSON bằng Python trực tiếp hoặc `utf-8-sig`.
20. Image `apache/spark` **không có numpy** → `pyspark.ml` lỗi import. Đã thêm vào Dockerfile. (`python3 -c "import pyspark"` ngoài spark-submit báo không có pyspark — bình thường, pyspark nằm trong /opt/spark.)
21. Silhouette cao chưa chắc phân cụm tốt: không log1p cho 0.77 nhưng cụm 2 khách → luôn xem cluster size.
22. Số thứ tự Cluster (0..K−1) do khởi tạo quyết định, không có ý nghĩa; đổi seed/K có thể đổi số thứ tự.
23. `Set-Content -Encoding utf8` (PowerShell 5.1) ghi file có BOM → dùng Write tool / Python để ghi docs.
24. Matplotlib: nhãn giá trị dễ đè lên chấm median / bị cắt ở mép trên → đặt nhãn trên max(mean, median) và nới ylim; đã xem lại từng PNG.
25. Font mặc định DejaVu Sans của Matplotlib hiển thị đúng tiếng Việt có dấu (đã kiểm tra trên PNG).
26. `hdfs dfs -chown -R / -chmod -R` trên thư mục gốc project đổi luôn owner/quyền file raw → chỉ áp cho thư mục (đã sửa `init_hdfs.py`).
27. Môi trường làm việc **chặn xóa hàng loạt trên HDFS** (`hdfs dfs -rm -r` nhiều thư mục) → kiểm tra "chạy lại được" bằng overwrite + mtime/UUID mới thay vì xóa trước.
28. Mở `web/index.html` trực tiếp (file://) → fetch JSON bị chặn → luôn mở qua `run_web_demo.py`.
29. Windows: `SO_REUSEADDR` cho phép mở trùng port đang bận → `run_web_demo.py` tắt cờ này trên win32 để báo lỗi rõ.
30. Port 4040 được Docker publish nên luôn "listen" trên host, nhưng không có job thì không trả HTTP (trang không mở được — bình thường).
31. Edge headless: cần `--user-data-dir` riêng khi Edge đang mở; chiều rộng cửa sổ tối thiểu ~500px (ảnh 390px bị cắt không phải lỗi layout).

## 11. Lệnh hay dùng

```bash
# Local (Windows .venv)
.venv\Scripts\python scripts\download_dataset.py [--force]
.venv\Scripts\python scripts\profile_dataset.py [--path hdfs://...]

# Cluster
docker compose up -d | ps | logs <service> | stop | down      # KHÔNG dùng "down -v" trừ khi muốn xóa HDFS
.venv\Scripts\python scripts\upload_to_hdfs.py [--force]                 # Phase 3: raw local -> HDFS
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_hdfs_ingestion.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/preprocessing/clean_transactions.py   # Phase 4 (~50 s)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_processed_data.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/feature_engineering/build_rfm.py      # Phase 5 (~25 s)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_rfm.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/clustering/evaluate.py   # Phase 7: K 2..6 × 2 variant (~40 s)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/clustering/kmeans.py     # Phase 6: model cuối K = selected_k (~15 s)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_clustering.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/analysis/cluster_analysis.py   # Phase 8 (~20 s)
.venv\Scripts\python -m src.analysis.charts                 # Phase 10: 7 PNG
.venv\Scripts\python scripts\init_hdfs.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 <script.py> [args]
docker compose exec namenode hdfs dfs -ls -R /data/customer-segmentation
docker compose exec namenode hdfs fsck <path> -files -blocks -locations
docker compose exec namenode hdfs dfsadmin -report
docker compose cp <local-file> namenode:/tmp/<file>          # rồi: hdfs dfs -put /tmp/<file> <hdfs-dir>

# Web UI: http://localhost:9870 (HDFS) · :9864 (DataNode) · :8080 (Spark Master) · :8081 (Worker) · :4040 (job đang chạy)
```
