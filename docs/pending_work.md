# Pending Work — Việc cần làm tiếp

> Đọc sau `docs/claude_context.md`. File này cho biết **Phase tiếp theo làm gì, đã chuẩn bị gì, kiểm tra bằng số nào**.
> Cập nhật lần cuối: **2026-10-01 — UI redesign Web Demo (sau khi pipeline Big Data đã hoàn thành; không đổi roadmap kỹ thuật)**.
>
> ## ✅ All implementation phases completed.
>
> Không còn Phase nào trong roadmap. Việc còn lại (nếu muốn) là polish/chuẩn bị, xem "Remaining tasks (optional)".
>
> Roadmap **Phase 3 → Final** dưới đây là **yêu cầu của người dùng** (gửi trong chat 2026-10-01, thay thế đặc tả cũ ngày 2026-09-30).
> Thay đổi chính so với đặc tả cũ: **gộp** Phase 6+7 và 8+10, **bỏ** Phase 9 (scale-up/benchmark) và Phase 11 (Dashboard),
> Phase 12 chỉ còn kiểm tra end-to-end, **gộp** Phase 13+14 (viva tối thiểu 20 câu → `docs/14_viva.md`), K thử **2..6**.
> Mỗi Phase có 2 phần: **Yêu cầu (từ người dùng)** = bắt buộc; **Đã chuẩn bị / lưu ý khi làm** = ghi chú kỹ thuật để triển khai.

---

## Completed phases

- **Phase 0 — Project skeleton:** cấu trúc thư mục, README, requirements, .gitignore, config.yaml, .env.example,
  stub modules, `01_problem_definition.md`, skeleton `PROJECT_EXPLANATION.md`.
- **Phase 1 — Dataset:** `download_dataset.py` (tải UCI, retry, kiểm tra HTTP/zip, `--force`), Excel → CSV,
  `profile_dataset.py` (PySpark), `csv_reader.py`, `spark_session.py`, `02_dataset.md` với số liệu thật.
- **Phase 2 — Docker + Hadoop HDFS + Spark:** `docker-compose.yml` (namenode, datanode, spark-master, spark-worker),
  `docker/hadoop/hadoop.env`, `docker/spark/{Dockerfile,spark-defaults.conf}`, `init_hdfs.py`, `hdfs_loader.py`,
  `spark_hdfs_smoke.py` (PASS), `04_architecture.md`.
- **Phase 3 — HDFS Ingestion (2026-10-01):** `scripts/upload_to_hdfs.py` (upload + không upload trùng + verify size/sha256),
  `scripts/check_hdfs_ingestion.py` (HDFS → Spark → schema → count → sample, **7/7 PASS**), `hdfs_loader.py` (`block_locations`,
  `read_raw_transactions(..., with_corrupt_record)`), `04_architecture.md` mục 7, `PROJECT_EXPLANATION.md`, README.
  Raw trên HDFS: 94,268,848 bytes, 1 block, 1,067,371 dòng × 8 cột STRING, 4 partition.
- **Phase 4 — Preprocessing (2026-10-01):** `src/preprocessing/clean_transactions.py` (9 rule, mỗi rule ghi detection/reason/handling/
  before/removed/after; hủy + dòng mua khớp 1-1; 25 mã không phải sản phẩm; `Amount`), `scripts/check_processed_data.py` (**10/10 PASS**),
  `config.yaml: preprocessing.*`, `03_preprocessing.md`. Processed: 770,563 dòng, 5,839 khách, Parquet 4 file trên HDFS.
- **Phase 5 — RFM (2026-10-01):** `src/feature_engineering/build_rfm.py` (AnalysisDate 2011-12-10, R/F/M, thống kê, outlier IQR, skew),
  `scripts/check_rfm.py` (**11/11 PASS**, tính lại bằng Spark SQL: 0 sai lệch), `config.yaml: rfm.*`, `05_rfm.md`.
  RFM: 5,839 khách, Parquet 1 file trên HDFS `rfm/`.
- **Phase 6+7 — Scaling + K-Means + Evaluation (2026-10-01):** numpy vào image Spark; `src/clustering/evaluate.py` (K 2..6 × có/không log1p),
  `src/clustering/kmeans.py` (log1p → StandardScaler → KMeans, model cuối), `scripts/check_clustering.py` (**10/10 PASS**),
  `config.yaml: clustering.*` (**selected_k = 4**), `06_kmeans.md`, `07_evaluation.md`. K = 4: Silhouette 0.5325, hội tụ 27 vòng,
  cụm 1,179 / 1,957 / 1,261 / 1,442 → HDFS `output/clustering/`.
- **Phase 8+10 — Cluster Analysis + Visualization (2026-10-01):** `src/analysis/cluster_analysis.py` (Spark: profile mean + median R/F/M,
  Tenure, % khách, % doanh thu → HDFS `output/analysis/cluster_profile` + CSV/JSON local, 7/7 check PASS), `src/analysis/charts.py`
  (Matplotlib — **người dùng chọn**, 7 PNG trong `output/reports/charts/`), `08_business_analysis.md` (Data Finding / Business
  Interpretation / Recommendation từng cụm). Phát hiện chính: Cluster 0 = 20.19% khách, 73.48% doanh thu.
- **Phase 12 — Final End-to-End (2026-10-01):** chạy lại 13 bước (init → smoke → upload → ingestion → preprocessing → RFM → evaluate →
  kmeans → analysis → charts + các check), **13/13 exit 0, mọi check PASS**, ≈ 294 s. Output tạo lại và **giống hệt** lần trước
  (JSON/CSV cùng nội dung, 7 PNG giống từng byte). Sửa 1 lỗi: `init_hdfs.py` dùng `-R` làm đổi owner/quyền file raw → chỉ áp cho thư mục,
  khôi phục raw `hadoop` / `rw-r--r--`. Docs: `04_architecture.md` mục 9.
- **Phase 13+14 — Documentation + Viva (2026-10-01):** **người dùng chọn xóa** file không dùng: `src/pipeline.py`, `scripts/generate_scaled_dataset.py`,
  `dashboard/`, `notebooks/` (3 notebook chỉ có tiêu đề); bỏ `jupyter`, `plotly`, `streamlit` khỏi requirements, `STREAMLIT_PORT` khỏi `.env.example`.
  **README viết lại** làm trang giới thiệu project (bài toán, kết quả, kiến trúc, cài đặt, 13 bước chạy, cấu trúc, tài liệu, hạn chế,
  troubleshooting; 3 biểu đồ chép vào `docs/images/`). `PROJECT_EXPLANATION.md` thêm Big Data characteristics + Limitations, bỏ mục cũ.
  `01_problem_definition.md` thêm Result; sửa câu cũ trong `02`, `04`, `05`. **`docs/14_viva.md`**: tóm tắt 1 phút, cheat sheet,
  **33 câu hỏi** Q/A/Why, demo script 7 phút, final checklist. Kiểm tra lại: compile OK, smoke + 4 check PASS, raw HEALTHY.

### Phần bổ sung sau roadmap (không đánh số Phase)

- **Web Demo — presentation layer (2026-10-01):** được thêm **sau khi** hoàn thành roadmap chính, chỉ để trình bày kết quả.
  **Không ảnh hưởng pipeline Big Data:** không sửa HDFS/Spark/preprocessing/RFM/K-Means, không chạy lại mô hình, không xóa output,
  không đổi kết quả; không database/auth/Docker riêng/Streamlit.
  - Công nghệ: HTML + CSS + Vanilla JS + server tĩnh `http.server` (127.0.0.1). Giao diện tiếng Việt.
  - File mới: `scripts/build_web_summary.py`, `scripts/run_web_demo.py`, `web/index.html`, `web/style.css`, `web/app.js`,
    `web/cluster_interpretation.json`; output mới duy nhất: `output/reports/summary.json`.
  - Chạy: `.venv\Scripts\python scripts\run_web_demo.py` → **http://127.0.0.1:8000/web/** (port bận → `--port 8001`).
  - URL hạ tầng (mở trực tiếp trong trình duyệt, không nằm trên web): HDFS **http://localhost:9870** · Spark Master **http://localhost:8080** ·
    Spark Application **http://localhost:4040** (chỉ khi có `spark-submit` đang chạy).
  - Theo yêu cầu người dùng: **đã xóa mục "Hạ tầng Big Data"** (nút + kiểm tra trạng thái) và **phóng to giao diện** để trình chiếu.
  - Test: route 200 (data/, configs/ → 404); 62/62 số trên trang khớp output gốc (kiểm tra lại sau khi sửa); 7 ảnh không hỏng;
    `output/` giống hệt bản sao lưu (chỉ thêm `summary.json`); bố cục 1600px / 520px OK.
  - Docs: README mục 7 (Web Demo) + mục 8 (Demo Flow), `claude_context.md` mục "Web Demo".
- **UI redesign cho Web Demo sau khi pipeline Big Data đã hoàn thành (2026-10-01):** giao diện chuyển sang kiểu hệ thống phân tích dữ liệu
  nội bộ / báo cáo BI (header mỏng, sidebar, bảng là trọng tâm, 1 màu nhấn `#245B73`, không gradient/shadow/glow/hero, animation tối thiểu).
  Sửa `web/index.html`, `web/style.css`, `web/app.js`. **Không thay đổi** dữ liệu, `summary.json`, scripts, pipeline, kết quả, roadmap kỹ thuật.
  Mục "Môi trường thực nghiệm" (bảng 9870 / 8080 / 4040) được thêm rồi **xóa theo yêu cầu người dùng** — khi demo mở trực tiếp
  http://localhost:9870, :8080, :4040 trong tab mới. Test: 62/62 số khớp, `output/` không đổi.
- **Kết thúc công việc (2026-10-01):** người dùng yêu cầu dừng tại đây. Không còn việc bắt buộc.

## Current phase

- **Không còn Phase nào** — project hoàn thành; Web Demo bổ sung đã xong, chờ người dùng xác nhận.
- Khi mở chat mới: đọc `claude_context.md`, chạy checklist mục 3 nếu cần demo; **không sửa code khi chưa được yêu cầu**.

## Next phase

- Không có. Nếu người dùng yêu cầu thêm, xem "Remaining tasks (optional)" bên dưới.

## Remaining tasks (optional — không bắt buộc theo roadmap)

- Luyện demo theo **README mục 8 (Demo Flow, dùng Web Demo)** hoặc `docs/14_viva.md` mục 4; mở sẵn Web Demo + Web UI 9870/8080.
- Sau khi chạy lại pipeline (vd đổi K), chạy lại `run_web_demo.py` để tạo lại `summary.json`; nếu K khác 4, tên nhóm trên web tự ẩn
  cho tới khi cập nhật `web/cluster_interpretation.json` theo `docs/08_business_analysis.md`.
- Nếu cần nộp báo cáo Word/slide: dùng số liệu và 7 PNG trong `output/reports/charts/` (hoặc `docs/images/`).
- Config còn vài khóa không dùng (`paths.processed`, `paths.sample`, `paths.output_rfm`) — vô hại, có thể dọn.
- Nếu biểu đồ được vẽ lại, chép lại 3 ảnh vào `docs/images/` cho README.

## Remaining tasks (roadmap)

| Phase | Nội dung | Trạng thái |
|---|---|---|
| 3 | HDFS Ingestion: Raw → HDFS → Spark DataFrame | ✅ Done |
| 4 | Preprocessing (PySpark → Parquet trên HDFS) | ✅ Done |
| 5 | RFM | ✅ Done |
| 6 + 7 | Scaling + K-Means + Evaluation (K = 2..6, Silhouette, cluster size, chọn K) | ✅ Done (K = 4) |
| 8 + 10 | Cluster Analysis + Visualization (≥ 4 biểu đồ, không dashboard) | ✅ Done (7 biểu đồ) |
| ~~9~~ | ~~Scale-up + benchmark~~ — **BỎ** (ghi là giới hạn thực nghiệm) | ❌ |
| ~~11~~ | ~~Dashboard Streamlit~~ — **BỎ** | ❌ |
| 12 | Final end-to-end check (chỉ sửa lỗi cần thiết) | ✅ Done |
| 13 + 14 | Documentation + Viva (`docs/14_viva.md`, ≥ 20 câu) | ✅ Done (33 câu) |
| — | Web Demo (bổ sung sau roadmap, presentation layer — không đánh số lại roadmap) | ✅ Done |

## Blockers

None

---

## Quy tắc bắt buộc (từ người dùng)

1. Chỉ thực hiện **Phase hiện tại**, không tự nhảy Phase.
2. Trước mỗi Phase phải đọc: `README.md` · `docs/claude_context.md` · `docs/pending_work.md` · code/config liên quan.
3. Luôn kiểm tra trạng thái thực tế, không suy đoán.
4. Không tạo dữ liệu giả hoặc số liệu giả.
5. Không mở rộng scope.
6. Không thêm Kafka, Flink, Redis, Airflow, Kubernetes, FastAPI, React hoặc công nghệ không cần thiết.
7. Không xây Dashboard/Frontend nếu chưa được yêu cầu.
8. Xử lý dữ liệu chính bằng **PySpark**, không dùng Pandas để xử lý toàn bộ dataset.
9. Raw dataset phải được giữ nguyên.
10. Sau mỗi Phase cập nhật: `docs/claude_context.md` · `docs/pending_work.md` · documentation liên quan.
11. Cuối mỗi Phase báo cáo kết quả chi tiết (tiếng Việt) và **STOP**.
12. Không thực hiện Phase tiếp theo cho đến khi người dùng xác nhận.

**Khi chuyển conversation/context:** đọc `claude_context.md` → đọc `pending_work.md` → kiểm tra code thực tế → báo cáo trạng thái →
**không sửa code cho đến khi người dùng xác nhận.**

**Thứ tự ưu tiên:** HDFS → Spark → Preprocessing → RFM → K-Means → Evaluation → Cluster Analysis → Visualization → Documentation → Viva.

**Mỗi Phase:** IMPLEMENT → TEST → DOCUMENT → REPORT → STOP → WAIT FOR APPROVAL.

Mục tiêu: **hoàn thành một pipeline Big Data chạy thật trong thời gian giới hạn**, không phải xây production system.

**Nội dung báo cáo cuối Phase:** files created/modified · command đã chạy · kết quả test (số thật) · lỗi gặp và cách sửa ·
kiến thức cần hiểu · câu hỏi giảng viên có thể hỏi · Phase tiếp theo.

---

## Phase 3 — HDFS Ingestion ✅ DONE (2026-10-01)

**Pipeline:** `Raw Dataset → HDFS → Spark DataFrame`. **Output:** HDFS raw + Spark-readable dataset.

### Kết quả thật

| Kiểm tra | Kết quả |
|---|---|
| HDFS path | `hdfs://namenode:8020/data/customer-segmentation/raw/online_retail_II.csv` |
| Size HDFS = local | 94,268,848 bytes · sha256 `b0dab2a0…3138e8c` khớp |
| Block | 1 block, replication 1, HEALTHY |
| Upload lần 2 | SKIP (không upload trùng) · file tạm trong container đã xóa |
| Spark `count()` | **1,067,371** (khớp metadata Phase 1) |
| Schema | 8 cột STRING: `Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country` |
| Partition | **4** (1 block nhưng split ≈ 23.5 MB vì 4 core) |
| Corrupt / null | 0 corrupt · null Customer ID 243,007 · Description 4,382 (khớp Phase 1) |
| Dòng đầu | `489434, 85048, 15CM CHRISTMAS GLASS BALL 20 LIGHTS, 12, 2009-12-01 07:45:00, 6.95, 13085, United Kingdom` (5 dòng đầu khớp CSV local) |
| `check_hdfs_ingestion.py` | **RESULT: PASS** (7/7) → `output/reports/hdfs_ingestion_check.json` |

Lựa chọn: giữ `/tmp/phase2-smoke/` trên HDFS (input mặc định của `spark_hdfs_smoke.py`).

Phần dưới là đặc tả + ghi chú gốc của Phase 3 (giữ để tham chiếu).

### Yêu cầu (từ người dùng)

- Upload dataset vào HDFS.
- Kiểm tra HDFS path/file.
- PySpark đọc dữ liệu **trực tiếp từ HDFS**.
- Kiểm tra schema, record count, sample.
- Chứng minh HDFS + Spark hoạt động thực tế.
- **Không** preprocessing / RFM / K-Means.

### Đã chuẩn bị sẵn

- Thư mục đích đã có: `/data/customer-segmentation/raw/` (owner `spark`).
- `scripts/upload_to_hdfs.py` đang là **stub** → hoàn thiện ở Phase này.
- `hdfs_loader.read_raw_transactions(spark)` mặc định đọc `hdfs_uri(hdfs.raw_dir + "/" + dataset.raw_filename)`
  = `hdfs://namenode:8020/data/customer-segmentation/raw/online_retail_II.csv`; có `path_exists`, `list_dir`.
- Mẫu gọi HDFS CLI từ host: `scripts/init_hdfs.py` (`docker compose exec -T namenode hdfs ...` qua `subprocess`).
- Cách upload đã test ở Phase 2: `docker compose cp <file> namenode:/tmp/<file>` → `hdfs dfs -put [-f] /tmp/<file> <dir>`.
- `scripts/profile_dataset.py --path hdfs://...` chạy được trên cluster để đối chiếu với Phase 1.

### Gợi ý triển khai / điểm cần chú ý

- `upload_to_hdfs.py`: kiểm tra file local tồn tại + không rỗng → tạo thư mục HDFS nếu chưa có → upload → kiểm tra sau upload →
  **không upload trùng** → log rõ ràng → exit code 0/1.
- "Không upload trùng": so **size** (`hdfs dfs -stat %b`) và **checksum**. `hdfs dfs -checksum` (MD5-of-CRC) **không so sánh được**
  trực tiếp với sha256 local → sha256 local (Python) vs `hdfs dfs -cat <file> | sha256sum` trong container.
  Giống → skip; khác → báo lỗi hoặc ghi đè khi có `--force`.
- Xóa file tạm `/tmp/<file>` trong container namenode sau khi put.
- File 94.3 MB < block 128 MB → dự kiến **1 block** (kiểm tra bằng `hdfs fsck -files -blocks -locations`).
- 1 block nhưng Spark vẫn chia nhiều partition: `maxSplitBytes = min(128 MB, max(4 MB, totalBytes / defaultParallelism))`
  ≈ 94.3 MB / 4 core ≈ 23.6 MB → dự kiến ~4 partition. **Phải đo thật** (`df.rdd.getNumPartitions()`).
- Giải thích vì sao **raw giữ CSV**, **processed dùng Parquet** (columnar, có schema + kiểu, nén tốt, column pruning, predicate pushdown, đọc song song).
- Nếu chạy `profile_dataset.py` trên cluster: `--out output/reports/dataset_profile_hdfs.json`, kiểm tra user `spark` ghi được vào bind mount `/opt/project/output`.
- File test `/tmp/phase2-smoke/` trên HDFS: xóa hoặc giữ cho `spark_hdfs_smoke.py` — ghi rõ lựa chọn.
- Docs: `04_architecture.md`, `PROJECT_EXPLANATION.md`, `claude_context.md`, `pending_work.md`.

### Số liệu để đối chiếu (từ Phase 1)

| Kiểm tra | Giá trị đúng |
|---|---|
| Size file trên HDFS | 94,268,848 bytes |
| `count()` từ HDFS | 1,067,371 |
| Số cột / schema | 8 cột STRING: `Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country` |
| Dòng đầu | `489434, 85048, 15CM CHRISTMAS GLASS BALL 20 LIGHTS, 12, 2009-12-01 07:45:00, 6.95, 13085, United Kingdom` |

---

## Phase 4 — Preprocessing ✅ DONE (2026-10-01)

**Output:** cleaned transaction dataset (Parquet trên HDFS `/data/customer-segmentation/processed/`).

### Kết quả thật (chi tiết `docs/03_preprocessing.md`)

| # | Rule | Detected in raw | Removed | After |
|---|---|---:|---:|---:|
| 1 | invalid_invoice_date | 0 | 0 | 1,067,371 |
| 2 | invalid_values | 0 | 0 | 1,067,371 |
| 3 | duplicate_rows | 34,335 | 34,335 | 1,033,036 |
| 4 | cancelled_invoices (19,104 dòng hủy + 6,146 dòng mua khớp) | 19,494 | 25,250 | 1,007,786 |
| 5 | non_sale_invoices (`A…`) | 6 | 6 | 1,007,780 |
| 6 | quantity_non_positive | 22,950 | 3,393 | 1,004,387 |
| 7 | unit_price_non_positive | 6,207 | 2,621 | 1,001,766 |
| 8 | missing_customer_id | 243,007 | 228,487 | 773,279 |
| 9 | non_product_stockcode (25 mã) | 5,912 | 2,716 | **770,563** |

- Output: 4 file Parquet (11,033,643 bytes), 770,563 dòng, **5,839 khách**, 36,338 hóa đơn, tổng Amount **16,521,624.51**,
  InvoiceDate 2009-12-01 07:45 → 2011-12-09 12:50. 0 NULL, 0 trùng, 0 giá trị không hợp lệ.
- `scripts/check_processed_data.py`: **RESULT: PASS (10/10)**, đọc lại Parquet bằng Spark. Chạy preprocessing 2 lần → cùng số liệu.

### Quyết định đã chốt (7 câu hỏi mở trước đây)

1. Đổi tên `InvoiceNo`, `UnitPrice`, `CustomerID` từ processed; `config.yaml: rfm.customer_col = CustomerID`; docstring `build_rfm.py` đã sửa.
2. Loại **mọi** dòng trùng 8 cột (34,335), kể cả 11,812 dòng ngoài vùng chồng lấn — không phân biệt được với lỗi ghi trùng.
3. **Người dùng chọn:** loại StockCode không phải sản phẩm theo **danh sách tường minh 25 mã** (`preprocessing.non_product_stockcodes`).
4. **Người dùng chọn:** bỏ dòng hủy **+ dòng mua khớp 1-1** (cùng CustomerID, StockCode, |Quantity|, UnitPrice, mua ≤ lần hủy cuối).
5. Không trim Description.
6. Thứ tự rule như bảng; báo cáo kèm `detected_in_raw` vì kết quả cuối không phụ thuộc thứ tự.
7. Báo cáo rule: `output/reports/preprocessing_report.json` (local qua bind mount).

Phần dưới là đặc tả + ghi chú gốc của Phase 4 (giữ để tham chiếu).

### Yêu cầu (từ người dùng)

- Dùng PySpark xử lý:
  - missing CustomerID;
  - duplicate;
  - Quantity <= 0;
  - UnitPrice <= 0;
  - invalid InvoiceDate;
  - cancelled/invalid transactions theo rule phù hợp;
  - tạo **`Amount = Quantity * UnitPrice`**.
- Ghi nhận **before/after statistics**.
- Lưu processed data bằng **Parquet vào HDFS**.
- Cập nhật **`docs/03_preprocessing.md`**.

### Đã chuẩn bị sẵn

- Code: `src/preprocessing/clean_transactions.py` (stub; docstring cũ ghi `TotalPrice` → đổi thành `Amount`).
- Đọc input: `hdfs_loader.read_raw_transactions(spark)` — **đã kiểm tra ở Phase 3** (1,067,371 dòng, 4 partition).
  Ghi output: `hdfs_uri(cfg["hdfs"]["processed_dir"])`. Chạy bằng spark-submit trong container (như `check_hdfs_ingestion.py`).
- Số "records before" của rule đầu tiên = **1,067,371** (`output/reports/hdfs_ingestion_check.json`).
- Mẫu ép kiểu an toàn (ANSI mode): `add_typed_columns()` trong `scripts/profile_dataset.py`
  (`try_cast(Quantity AS INT)`, `try_cast(Price AS DOUBLE)`, `try_to_timestamp(InvoiceDate, 'yyyy-MM-dd HH:mm:ss')`).
- Mẫu phát hiện vấn đề (hủy, A-invoice, StockCode đặc biệt, duplicate): hàm `profile()` trong `scripts/profile_dataset.py`.
- Mỗi rule nên ghi: detection · reason · handling · records before · removed · after (không âm thầm xóa).
- Test: schema · row count · missing · duplicates · invalid values · output Parquet · **đọc lại Parquet bằng Spark**.

### Quyết định còn mở trước Phase 4 (đã chốt ở trên)

1. **Đổi tên cột** ở processed: `Invoice→InvoiceNo`, `Price→UnitPrice`, `Customer ID→CustomerID`
   → cập nhật `config.yaml: rfm.customer_col` (đang là `Customer ID`) và docstring `build_rfm.py` (`TotalPrice` → `Amount`).
2. **Duplicate:** 22,523 dòng chồng lấn sheet **bắt buộc** loại. 11,812 dòng trùng còn lại: loại hay giữ? — nêu lý do.
3. **StockCode không phải sản phẩm** (POST, DOT, M, C2, D, S, BANK CHARGES, ADJUST, AMAZONFEE, …): có loại không? Ảnh hưởng Monetary.
   Heuristic regex bắt nhầm vài mã sản phẩm thật (vd `DCGS0058`) → cần danh sách rõ ràng.
4. **Hóa đơn hủy:** loại bỏ (đơn giản) hay trừ vào Monetary (net revenue)?
5. Trim khoảng trắng Description — có cần không (Description không dùng cho RFM).
6. **Thứ tự rule** ảnh hưởng "records removed" → ghi rõ thứ tự. 3,457 dòng Quantity < 0 không phải hủy và 6 dòng hóa đơn `A`
   **đều không có Customer ID** → nếu lọc Customer ID trước, các rule sau đếm được 0 cho nhóm này.
7. Lưu báo cáo từng rule: JSON trong `output/reports/` (local, qua bind mount) và/hoặc HDFS.

### Số liệu raw để đối chiếu (Phase 1, trên toàn bộ 1,067,371 dòng, chưa lọc gì)

| Vấn đề | Số dòng |
|---|---:|
| Customer ID NULL | 243,007 |
| Duplicate thừa (8 cột) | 34,335 (22,523 overlap + 11,812 khác) |
| Invoice bắt đầu `C` | 19,494 |
| Invoice bắt đầu `A` | 6 |
| Quantity < 0 / = 0 | 22,950 / 0 |
| Price < 0 / = 0 | 5 / 6,202 |
| InvoiceDate không parse được | 0 |
| StockCode không phải sản phẩm (heuristic) | 6,094 |

---

## Phase 5 — RFM ✅ DONE (2026-10-01)

**Output:** `CustomerID | Recency | Frequency | Monetary` → HDFS/Parquet `/data/customer-segmentation/rfm/`.

### Kết quả thật (chi tiết `docs/05_rfm.md`)

- **AnalysisDate = 2011-12-10** (= to_date(max InvoiceDate processed 2011-12-09 12:50) + 1 ngày; config `rfm.analysis_date: null`).
- Recency = `datediff(AnalysisDate, to_date(max(InvoiceDate)))` (ngày) · Frequency = `countDistinct(InvoiceNo)` · Monetary = `round(sum(Amount), 2)`.
- 770,563 giao dịch → **5,839 khách**; schema `CustomerID` string, `Recency` int, `Frequency` bigint, `Monetary` double; 1 file Parquet 74,485 bytes.
- Thống kê: Recency 1–739 (median 96) · Frequency 1–369 (median 3, skew 11.96) · Monetary 2.90–579,128.64 (median 851.01, skew 26.61).
  Outlier IQR (giữ lại): F 421, M 616. Skew sau log1p: R −0.45, F 1.01, M 0.24.
- `scripts/check_rfm.py`: **RESULT: PASS (11/11)** — tính lại bằng Spark SQL và so từng khách: 0 sai lệch; Σ F = 36,338; Σ M = Σ Amount.

Phần dưới là đặc tả + ghi chú gốc của Phase 5 (giữ để tham chiếu).

### Yêu cầu (từ người dùng)

- Từ processed transactions tạo customer-level features: `Recency`, `Frequency`, `Monetary`.
- Phải xác định rõ: **AnalysisDate** · **Frequency definition** · **Monetary calculation**.
- Kiểm tra: customer count · RFM statistics · null/duplicate · giá trị bất thường.
- Lưu RFM vào HDFS/Parquet.
- Cập nhật **`docs/05_rfm.md`**.

### Đã chuẩn bị sẵn / gợi ý

- Code: `src/feature_engineering/build_rfm.py` (stub; docstring đã dùng `InvoiceNo`, `Amount`).
- **Input:** `hdfs_loader.read_parquet(spark, hdfs_uri(cfg["hdfs"]["processed_dir"]))` — 770,563 dòng, cột
  `CustomerID` (string), `InvoiceNo`, `InvoiceDate` (timestamp_ntz), `Amount` (double). Chạy bằng spark-submit như `clean_transactions.py`.
- **Frequency:** đề xuất `countDistinct(InvoiceNo)` (số lần mua thực tế), không phải số dòng — giải thích lựa chọn.
- **Monetary:** tổng `Amount` của khách (Amount > 0 mọi dòng → Monetary > 0).
- **AnalysisDate:** `config.yaml: rfm.snapshot_date: null` = rule tự động max(InvoiceDate) + 1 ngày, tính trên dữ liệu **đã làm sạch**:
  max processed = **2011-12-09 12:50:00** (đo ở Phase 4). Cần chốt so theo **ngày** (`datediff`) hay theo giờ.
- Thống kê: `F.percentile_approx(col, 0.5)` (median), `F.stddev`, `F.skewness`; kiểm tra CustomerID không trùng, không null.
- Customer count kỳ vọng = **5,839** (`output/reports/preprocessing_report.json → summary.customers`) — đọc từ file, không hard-code.
- Outlier đã biết để xem ở phần "giá trị bất thường": khách 15098 (dòng 38,970.00 bị hủy qua mã `M`, không khớp được);
  12,244 dòng hủy không khớp → Monetary vài khách hơi cao (xem `03_preprocessing.md` mục 9).
- Notebook `notebooks/02_rfm_exploration.ipynb` (tùy chọn) — pandas được trên bảng RFM nhỏ (~vài nghìn dòng).

---

## Phase 6 + 7 — Scaling + K-Means + Evaluation (gộp)

**Pipeline:** `RFM → VectorAssembler → StandardScaler → K-Means → Silhouette`

### ✅ DONE (2026-10-01) — kết quả thật (chi tiết `docs/06_kmeans.md`, `docs/07_evaluation.md`)

- Image Spark thêm **numpy 2.2.6** (bắt buộc cho `pyspark.ml`), đã rebuild + recreate master/worker.
- `src/clustering/evaluate.py`: K = 2..6 × 2 cách scaling → `output/evaluation/k_evaluation.{json,csv}` + HDFS `evaluation/k_selection`.
  - log1p + StandardScaler: Silhouette 0.6266 / 0.5060 / **0.5325** / 0.5145 / 0.4944; cụm nhỏ nhất 39.8% / 21.3% / 20.2% / 7.7% / 7.7%.
  - Không log1p: 0.4146 / 0.6780 / 0.5549 / 0.7542 / 0.7728 nhưng K ≥ 3 có cụm 2–21 khách → loại.
- **Chọn K = 4** (log1p): Silhouette cao nhất trong K = 3..6, cụm ≥ 20%, 4 cụm khác rõ R/F/M; K = 2 quá thô. `config: selected_k: 4`.
- `src/clustering/kmeans.py`: model cuối K = 4, numIter 27/100 (hội tụ), WSSSE 4,859.15, Silhouette 0.5325 →
  HDFS `output/clustering/` (`CustomerID, Recency, Frequency, Monetary, Cluster`). Cụm: 1,179 / 1,957 / 1,261 / 1,442.
- `scripts/check_clustering.py`: **RESULT: PASS (10/10)** — Silhouette tính lại = 0.5325 = report = evaluate (tái lập).
- Docs: đổi `08_evaluation.md` → `07_evaluation.md`, xóa placeholder `07_experiment.md`, đổi `09_business_analysis.md` → `08_business_analysis.md`.

Phần dưới là đặc tả + ghi chú gốc của Phase 6+7 (giữ để tham chiếu).

### Yêu cầu (từ người dùng)

- Dùng **Spark MLlib**.
- Thử **K = 2..6**. Với mỗi K ghi **Silhouette Score** và **cluster size**.
- Chọn K dựa trên **kết quả thực nghiệm + khả năng diễn giải**. **Không tự chọn K trước.**
- Lưu **customer + cluster**.
- Giải thích: feature scaling · centroid · distance · assignment · iteration · convergence · Silhouette Score.
- Cập nhật **`docs/06_kmeans.md`**, **`docs/07_evaluation.md`**.

### Đã chuẩn bị / lưu ý khi làm

- Code: `src/clustering/kmeans.py` (stub `scale_features`, `train_kmeans`), `src/clustering/evaluate.py` (stub `silhouette_score`, `evaluate_k_range`).
- `config.yaml: clustering` hiện `features: [recency, frequency, monetary]` (chữ thường — **đổi thành `Recency, Frequency, Monetary`**
  cho khớp cột RFM / hằng `FEATURES` trong `build_rfm.py`), `k_min: 2, k_max: 10, seed: 42, max_iter: 20` → **đổi `k_max` thành 6**;
  thêm `selected_k` sau khi chọn.
- Input: RFM Parquet ở `/data/customer-segmentation/rfm/` — `hdfs_loader.read_parquet(spark, hdfs_uri(cfg["hdfs"]["rfm_dir"]))`,
  5,839 dòng (kỳ vọng đọc từ `output/reports/rfm_report.json → customers`). Frequency kiểu **bigint** → VectorAssembler nhận được.
  Output đề xuất: `/data/customer-segmentation/output/clustering/`
  (`CustomerID, Recency, Frequency, Monetary, Cluster` — lưu **R/F/M gốc**, không lưu vector scaled).
- **Skew thật từ Phase 5:** Recency 0.89 · Frequency **11.96** · Monetary **26.61**; sau `log1p`: −0.45 · 1.01 · 0.24.
  → đề xuất `log1p` rồi `StandardScaler` (withMean + withStd); **ghi rõ lý do**, so sánh mean/std trước/sau scaling,
  có thể so Silhouette có/không log1p. Outlier (F 421, M 616 khách theo IQR) được giữ lại từ Phase 5.
- Silhouette: `ClusteringEvaluator` (mặc định squared Euclidean). Seed cố định 42; MLlib KMeans khởi tạo k-means||, dừng theo `maxIter`/`tol`.
- `cache()` bảng scaled trước vòng lặp K. Thời gian chạy mỗi K **không còn bắt buộc** — có thể ghi thêm nếu tiện.
- Tiêu chí chọn K: Silhouette · không có cluster quá nhỏ · khả năng diễn giải. Nêu hạn chế Silhouette (ưu tiên cụm lồi, thường cao ở K nhỏ,
  phụ thuộc scaling/outlier, tính trên không gian đã scale).
- **Đổi tên doc:** đặc tả mới dùng `07_evaluation.md`; hiện có placeholder `07_experiment.md` + `08_evaluation.md` → xử lý khi làm Phase này
  (vd đổi `08_evaluation.md` → `07_evaluation.md`, bỏ `07_experiment.md` vì Phase 9 bị bỏ) — ghi rõ.
- Export bảng nhỏ (K/Silhouette/cluster size, JSON/CSV) ra `/opt/project/output/evaluation/` (bind mount) để Phase 8+10 vẽ biểu đồ trên host.
- **Không đặt tên business cho cluster** ở Phase này.

---

## Phase 8 + 10 — Cluster Analysis + Visualization (gộp)

### ✅ DONE (2026-10-01) — kết quả thật (chi tiết `docs/08_business_analysis.md`)

- **Người dùng chọn Matplotlib → PNG**; đã cài matplotlib 3.11.2 vào `.venv`, thêm `matplotlib>=3.8` vào requirements.
- `src/analysis/cluster_analysis.py` (Spark): profile 4 cụm (mean + median R/F/M, Tenure, % khách, % doanh thu, % mua 1 lần, so với overall)
  → HDFS `output/analysis/cluster_profile/` + `output/reports/cluster_profile.{csv,json}` + `output/clustering/customer_clusters.csv`; 7/7 check PASS.
- Profile: C0 1,179 (20.19%) median R 17 / F 13 / M 4,877.30, **73.48% doanh thu** · C1 1,957 (33.52%) 402 / 1 / 269.09, 69.09% mua 1 lần, 3.71% ·
  C2 1,261 (21.60%) 24 / 3 / 714.96, tenure median 220, 6.39% · C3 1,442 (24.70%) 185 / 4 / 1,444.58, 16.42%.
- Tên đề xuất (sau khi xem số liệu, chỉ trong docs): C0 giá trị cao/trung thành · C1 đã rời bỏ/mua 1 lần · C2 khách mới/tiềm năng · C3 có nguy cơ rời bỏ.
- `src/analysis/charts.py` (host): 7 PNG — số khách, Avg R, Avg F, Avg M (kèm median), % khách vs % doanh thu, boxplot R/F/M, K vs Silhouette.
  Đã mở xem từng ảnh, sửa nhãn đè/bị cắt.

Phần dưới là đặc tả + ghi chú gốc của Phase 8+10 (giữ để tham chiếu).

### Yêu cầu (từ người dùng)

- Cluster profile: `Cluster | CustomerCount | AvgRecency | AvgFrequency | AvgMonetary`. **Sau đó** mới diễn giải từng cluster.
- Phân biệt rõ: **Data Finding** · **Business Interpretation** · **Recommendation**.
- **Không tự gán nhãn cluster trước khi xem dữ liệu.**
- Tạo **tối thiểu 4 biểu đồ:**
  1. Customer count by cluster
  2. Average Recency
  3. Average Frequency
  4. Average Monetary
- **Không cần Streamlit/dashboard.**
- Cập nhật **`docs/08_business_analysis.md`**.

### Đã chuẩn bị / lưu ý khi làm

- Code: `src/analysis/cluster_analysis.py` (stub `profile_clusters`); chart: `dashboard/charts.py` (placeholder) hoặc module/script riêng — chốt khi làm.
- **Input:** HDFS `/data/customer-segmentation/output/clustering/` (5,839 khách, K = 4, cột `Cluster` 0..3) —
  `hdfs_loader.read_parquet(spark, hdfs_uri(f"{cfg['hdfs']['output_dir']}/clustering"))`. Số thứ tự cụm không có ý nghĩa.
  Trung bình đã thấy ở Phase 7 (để đối chiếu, chưa diễn giải): 0: R 28.33 / F 19.12 / M 10,296.27 · 1: 395.06 / 1.38 / 313.11 ·
  2: 29.03 / 3.03 / 837.64 · 3: 228.81 / 5.05 / 1,881.63.
- Profile dùng **R/F/M gốc**; nên thêm **median** (R/F/M lệch mạnh, mean bị kéo bởi khách lớn), tỉ trọng khách hàng, tỉ trọng doanh thu theo cụm.
- Output profile đề xuất: HDFS `/data/customer-segmentation/output/analysis/` + export bản nhỏ ra `output/` local để vẽ
  (vd `output/reports/cluster_profile.{json,csv}`). Bảng K/Silhouette đã có local: `output/evaluation/k_evaluation.csv`.
- **Host không đọc được HDFS** → vẽ trên host từ file export nhỏ (pandas được vì bảng đã aggregate).
  Plotly đã cài → lưu HTML (`write_html`). **Matplotlib chưa có trong requirements**, PNG từ Plotly cần `kaleido` → hỏi trước khi thêm dependency.
- Có thể thêm biểu đồ K vs Silhouette (từ Phase 6+7) nếu hữu ích; không vẽ biểu đồ chỉ để trang trí.
- Doc `08_business_analysis.md` đã có (placeholder, đổi tên từ `09_business_analysis.md` ở Phase 6+7).
- Không tuyên bố recommendation chắc chắn tăng doanh thu (chưa có experiment).

---

## Phase 9 — BỎ

- Không thực hiện scale-up 1M/5M/10M records hoặc benchmark.
- Không giả vờ dữ liệu nhỏ là Big Data cực lớn. Ghi rõ là **giới hạn thực nghiệm** trong docs/viva.
- `scripts/generate_scaled_dataset.py` (stub) không dùng → xử lý ở Phase 13+14 (xóa hoặc ghi chú "không triển khai").

## Phase 11 — BỎ

- Không xây Dashboard. Visualization bằng biểu đồ là đủ.
- `dashboard/app.py`, `utils.py` (placeholder) và `streamlit` trong `requirements.txt` → xử lý ở Phase 13+14 (xóa hoặc ghi chú) — hỏi người dùng.

---

## Phase 12 — Final End-to-End ✅ DONE (2026-10-01)

### Kết quả thật (chi tiết `docs/04_architecture.md` mục 9)

| # | Bước | Kết quả | Thời gian |
|---|---|---|---:|
| 1–4 | init_hdfs · smoke · upload · check_ingestion | OK · PASS · SKIP (giống hệt) · PASS (1,067,371 dòng) | 16.8 + 14.6 + 10.5 + 13.2 s |
| 5–6 | clean_transactions · check_processed_data | 770,563 dòng · PASS | 58.0 + 21.3 s |
| 7–8 | build_rfm · check_rfm | 5,839 khách, AnalysisDate 2011-12-10 · PASS (0 sai lệch) | 41.5 + 17.8 s |
| 9–11 | evaluate · kmeans · check_clustering | K = 4 Silhouette 0.5325 · 27 vòng · PASS | 42.9 + 21.4 + 15.8 s |
| 12–13 | cluster_analysis · charts | PASS · 7 PNG | 15.6 + 4.7 s |

- Bằng chứng tái lập: 20/20 file local tạo lại sau 07:13:09 UTC, so với bản sao lưu: **giống hệt**; HDFS output là file mới (UUID mới).
- Lỗi đã sửa: `scripts/init_hdfs.py` (`-R` → chỉ thư mục). Raw local + HDFS không đổi nội dung.
- Không làm: xóa output cũ trước khi chạy (môi trường chặn xóa hàng loạt HDFS — dùng overwrite thay thế), tải lại từ UCI, viết `src/pipeline.py`.

Phần dưới là đặc tả + ghi chú gốc của Phase 12 (giữ để tham chiếu).

### Yêu cầu (từ người dùng)

- Kiểm tra toàn bộ pipeline:
  `Dataset → HDFS → Spark → Preprocessing → RFM → Scaling → K-Means → Evaluation → Analysis → Visualization`.
- **Chỉ sửa lỗi cần thiết. Không refactor lớn.**
- Xác nhận **tất cả output tồn tại** và **có thể chạy demo**.

### Lưu ý khi làm

- `src/pipeline.py` (stub) **không bắt buộc** theo đặc tả mới; chạy lần lượt các script/stage là đủ. Nếu cần một entry point thì giữ đơn giản.
- Download/upload chạy trên **host**, các stage Spark chạy **trong container** (spark-submit), biểu đồ chạy trên **host**.
- Thứ tự chạy (đều đã có, xem "Demo commands"): `init_hdfs.py` → `upload_to_hdfs.py` → `check_hdfs_ingestion.py` → `clean_transactions.py`
  → `check_processed_data.py` → `build_rfm.py` → `check_rfm.py` → `evaluate.py` → `kmeans.py` → `check_clustering.py`
  → `cluster_analysis.py` → `python -m src.analysis.charts`. Mỗi check có `RESULT: PASS/FAIL` + exit code.
- Kiểm tra: `hdfs dfs -ls -R /data/customer-segmentation` + đối chiếu count với số đã chốt:
  raw 1,067,371 · processed 770,563 · RFM 5,839 · clustering 5,839 (K = 4, Silhouette 0.5325, sizes 1,179/1,957/1,261/1,442) · profile 4 dòng · 7 PNG.
  Chạy lại phải ra **đúng các số này** (đã chứng minh tái lập với seed 42 ở từng Phase).
- Raw trên HDFS và `data/raw/` không bao giờ bị xóa; output dẫn xuất ghi `mode("overwrite")` để chạy lại được.

---

## Phase 13 + 14 — Documentation + Viva (gộp) ✅ DONE (2026-10-01)

Kết quả: xem "Completed phases" ở đầu file. Bảng "Việc còn lại cho Phase 13+14" bên dưới **đã xử lý hết** (người dùng chọn xóa tất cả).
Phần dưới là đặc tả + ghi chú gốc (giữ để tham chiếu).

### Yêu cầu (từ người dùng)

- Hoàn thiện: `README.md` · `docs/` · `docs/claude_context.md` · `docs/pending_work.md`.
- Tạo **`docs/14_viva.md`**: câu hỏi/trả lời về Big Data · HDFS · NameNode/DataNode · Spark · PySpark DataFrame · preprocessing · RFM ·
  StandardScaler · K-Means · Silhouette · cluster interpretation · business recommendations · limitations.
- **Tối thiểu 20 câu hỏi bảo vệ**, ưu tiên câu liên quan trực tiếp đến implementation thực tế.

### Lưu ý khi làm

- Mọi con số đối chiếu với output thực tế, không placeholder. Tìm sót:
  `grep -rn "Sẽ được viết\|Sẽ được thực hiện\|NotImplementedError\|TODO" docs src scripts dashboard notebooks`.
- `PROJECT_EXPLANATION.md`: bỏ/cập nhật các mục Dashboard, Experiment (benchmark) cho khớp roadmap mới; thêm Limitations
  (dataset 1.07 triệu dòng / 94 MB **không lớn về Volume**; khía cạnh Big Data thể hiện qua kiến trúc HDFS + Spark; không benchmark scale-up; 1 DataNode, replication 1).
- README: overview · architecture · requirements · setup · Docker · HDFS · dataset · commands · output · troubleshooting
  (nguồn: mục 10 "Bẫy kỹ thuật" trong `claude_context.md`).
- Câu hỏi khó nên có: Dataset có thật sự Big Data không? · Vì sao HDFS/Spark mà không Pandas/database? · AnalysisDate là gì? ·
  Vì sao loại CustomerID null / xử lý hóa đơn hủy như vậy? · Vì sao scaling? · K chọn thế nào? · Cluster có phải label sẵn không? ·
  Recommendation có được chứng minh không? · Nếu dữ liệu tăng 100 lần thì sao?
- Khi xong: ghi `All implementation phases completed.` trong file này (giữ lại việc polish nếu còn).

### Việc còn lại cho Phase 13+14 (phát hiện khi rà ở Phase 12) — ✅ đã xử lý: xóa hết theo lựa chọn của người dùng

| File / mục | Tình trạng | Đề xuất |
|---|---|---|
| `src/pipeline.py` | stub `NotImplementedError("Phase 12")` | Xóa, hoặc thay bằng docstring trỏ tới thứ tự 13 bước (không viết orchestrator mới) |
| `scripts/generate_scaled_dataset.py` | stub `NotImplementedError("Phase 9")` | Xóa (Phase 9 đã bỏ) |
| `dashboard/app.py`, `charts.py`, `utils.py` | placeholder "Triển khai ở Phase 10/11" | Xóa thư mục `dashboard/` (Phase 11 bỏ; biểu đồ ở `src/analysis/charts.py`) |
| `streamlit`, `plotly` trong `requirements.txt` | Không dùng | Bỏ khỏi requirements (hoặc ghi chú không dùng) |
| `notebooks/02_rfm_exploration.ipynb`, `03_clustering_analysis.ipynb` | Chỉ có tiêu đề "Sẽ được thực hiện ở Phase 5/8" | Xóa, hoặc sửa tiêu đề trỏ tới docs/biểu đồ |
| `docs/PROJECT_EXPLANATION.md` | Mục `Dashboard`, `Experiment` cũ, `Limitations` trống | Sắp lại theo roadmap mới, viết Limitations |
| `README.md` | Cấu trúc thư mục còn `dashboard/`, `generate_scaled_dataset.py`; chưa có Troubleshooting | Cập nhật đầy đủ |

---

## Important technical decisions (ràng buộc mang sang các Phase sau)

1. **Spark 4.0.4 ở cả host và cluster** — không nâng/hạ version riêng lẻ. Hadoop 3.4.1.
2. Chạy job cluster bằng `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 ...`;
   **driver không chạy từ Windows**. Script trong container dùng Python **3.10** → không dùng cú pháp chỉ có ở 3.11+ (`except*`, `tomllib`).
3. Luôn dùng `try_cast` / `try_to_timestamp` (ANSI mode) và `TIMESTAMP_NTZ` + session tz UTC.
4. Raw giữ CSV + tên cột gốc + toàn bộ STRING; tên mới (nếu đổi) bắt đầu từ processed layer. Processed/RFM/output dùng **Parquet**.
5. Phase 4 **bắt buộc** loại 22,523 dòng chồng lấn giữa 2 sheet (nếu không doanh thu 01–09/12/2010 bị tính đôi).
6. Không đặt tên output HDFS bắt đầu bằng `_` hoặc `.`.
7. Git Bash: thêm `MSYS_NO_PATHCONV=1` trước `docker compose exec` có đường dẫn `/...`.
8. Không chạy `docker compose down -v` (xóa dữ liệu HDFS) trừ khi người dùng yêu cầu.
9. Không đụng container/volume của project khác trên máy.
10. Dataset và output không commit (`.gitignore`); project chưa là git repo — không tự `git init`.
11. **Host (Windows) không đọc được HDFS** → kết quả nhỏ cần cho biểu đồ phải được Spark export ra `/opt/project/output/...` (bind mount = `output/`).
12. HDFS layout: `raw/` · `processed/` · `rfm/` · `output/clustering/` · `output/analysis/` · `evaluation/` dưới `/data/customer-segmentation/`.
13. Không đặt tên business cho cluster trước Phase 8+10; khi đặt phải tách Data Finding / Business Interpretation / Recommendation.
14. Không kết luận vượt quá thực nghiệm (không "model tốt nhất", không "Spark nhanh hơn Pandas", không "chắc chắn tăng doanh thu").
15. Phase 9 (scale-up) và Phase 11 (dashboard) **đã bỏ** theo quyết định người dùng 2026-10-01 → ghi là giới hạn, không làm.
16. **Raw chỉ đọc qua `hdfs_loader.read_raw_transactions(spark)`** (HDFS). Không đọc `data/raw/` trong pipeline; không ghi vào HDFS `raw/`.
17. Upload raw bằng `upload_to_hdfs.py`: đã có bản giống hệt (size + sha256) → SKIP; khác → lỗi, chỉ ghi đè khi `--force`.
18. Số kỳ vọng trong các check lấy từ file kết quả thật (metadata.json, dataset_profile.json, hdfs_ingestion_check.json,
    preprocessing_report.json), không hard-code.
19. Processed schema: `InvoiceNo, StockCode, Description, Quantity (int), InvoiceDate (timestamp_ntz), UnitPrice (double),
    CustomerID (string), Country, Amount (double)`. Phase sau dùng tên này (hằng `OUTPUT_COLUMNS` trong `clean_transactions.py`).
20. Hóa đơn hủy: bỏ dòng hủy + dòng mua khớp 1-1; mã không phải sản phẩm: danh sách 25 mã trong config (cả hai do người dùng chọn).
21. Output Spark sau shuffle phải `coalesce(n)` trước khi ghi (tránh 200 file nhỏ); filter điều kiện có NULL phải bọc `coalesce(cond, False)`.
22. RFM: AnalysisDate = to_date(max InvoiceDate processed) + 1 ngày = **2011-12-10**; Recency theo ngày; Frequency = countDistinct(InvoiceNo);
    Monetary = round(sum(Amount), 2). Lưu giá trị gốc, giữ outlier. Cột: `CustomerID, Recency, Frequency, Monetary`.
23. Clustering: **log1p → StandardScaler(withMean, withStd) → KMeans(k-means||, seed 42, maxIter 100, tol 1e-4)**, **K = 4**
    (`clustering.selected_k`). Chọn K 2 bước: `evaluate.py` → điền `selected_k` → `kmeans.py`. Output `output/clustering/`
    = `CustomerID, Recency, Frequency, Monetary, Cluster` (R/F/M gốc). Số thứ tự Cluster không mang ý nghĩa.
24. Image Spark có numpy (cho `pyspark.ml`); sửa Dockerfile thì `docker compose build spark-master && docker compose up -d`.
25. Biểu đồ: Matplotlib PNG trên host (`python -m src.analysis.charts`) từ bảng nhỏ Spark export; tên cụm chỉ ở docs, code giữ "Cluster i".
    Profile có mean (đề bài) + median (vì lệch) + Tenure (mô tả). Recommendation không khẳng định tăng doanh thu.
26. `init_hdfs.py` chỉ chown/chmod **thư mục** (không -R); file raw HDFS giữ `hadoop` / `rw-r--r--`.
27. Pipeline chạy lại bằng 13 bước tuần tự (bảng Phase 12 ở trên / `04_architecture.md` mục 9); mọi output ghi overwrite, kết quả tái lập.
28. Web Demo chỉ là presentation layer: đọc `output/reports/summary.json` (gom từ output có sẵn) + PNG; không backend/API/DB;
    server chỉ bind 127.0.0.1 và chỉ phục vụ `/web/`, `/output/reports/`. Không được biến thành web production.
---

## Demo commands

Thứ tự dưới đây = luồng end-to-end đã kiểm tra ở Phase 12 (13 bước, ≈ 5 phút, mọi bước `RESULT: PASS` / exit 0).
Demo nhanh không cần chạy lại hết: mở `output/reports/charts/*.png`, `docs/08_business_analysis.md` và Web UI HDFS (9870) để xem file.

```bash
cd C:\Users\Quang\Desktop\BigData

# 1. Khởi động cluster (Docker Desktop phải đang chạy)
docker compose up -d
docker compose ps                          # namenode, datanode (healthy), spark-master, spark-worker (Up)

# 2. HDFS + Spark sẵn sàng (đã có từ Phase 2)
.venv\Scripts\python scripts\init_hdfs.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/spark_hdfs_smoke.py   # RESULT: PASS

# 3. Dataset local (Phase 1, chỉ cần khi chưa có data/raw/)
.venv\Scripts\python scripts\download_dataset.py

# 4. Phase 3 — Raw local -> HDFS -> Spark DataFrame
.venv\Scripts\python scripts\upload_to_hdfs.py            # lần đầu upload; lần sau in SKIP (đã có bản giống hệt); --force để ghi đè
docker compose exec namenode hdfs dfs -ls -R /data/customer-segmentation
docker compose exec namenode hdfs fsck /data/customer-segmentation/raw/online_retail_II.csv -files -blocks -locations   # 1 block, HEALTHY
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_hdfs_ingestion.py
#   -> schema 8 cột STRING, records=1,067,371, partitions=4, 5 dòng mẫu, RESULT: PASS

# 5. Phase 4 — Preprocessing: HDFS raw -> 9 rule (PySpark) -> HDFS processed/ (Parquet)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/preprocessing/clean_transactions.py
#   -> bảng before/removed/after từng rule, 1,067,371 -> 770,563 dòng, 4 file Parquet (~50 s)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_processed_data.py
#   -> đọc lại Parquet: schema, 0 null, 0 trùng, 0 invalid, RESULT: PASS
docker compose exec namenode hdfs dfs -ls /data/customer-segmentation/processed

# 6. Phase 5 — RFM: HDFS processed -> groupBy(CustomerID) -> HDFS rfm/ (Parquet)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/feature_engineering/build_rfm.py
#   -> AnalysisDate 2011-12-10, 5,839 khách, bảng thống kê R/F/M, outlier, khách mẫu (~25 s)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_rfm.py
#   -> tính lại bằng Spark SQL, 0 sai lệch, RESULT: PASS

# 7. Phase 6+7 — Scaling + K-Means + Evaluation (Spark MLlib)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/clustering/evaluate.py
#   -> bảng K = 2..6 (có/không log1p): Silhouette, cluster sizes, WSSSE, numIter, thời gian (~40 s)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/clustering/kmeans.py
#   -> model cuối K = 4: Silhouette 0.5325, 27 vòng, centroid, 4 cụm + TB R/F/M (~15 s)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_clustering.py
#   -> Silhouette tính lại = 0.5325, R/F/M không đổi, RESULT: PASS

# 8. Phase 8+10 — Cluster Analysis (Spark) + Visualization (host)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/analysis/cluster_analysis.py
#   -> bảng profile 4 cụm (mean, median, % khách, % doanh thu, tenure), 7 check, RESULT: PASS (~20 s)
.venv\Scripts\python -m src.analysis.charts
#   -> 7 PNG trong output/reports/charts/ (mở bằng trình xem ảnh khi demo)

# 9. Web Demo (bổ sung sau roadmap — chỉ đọc output, không ảnh hưởng pipeline)
.venv\Scripts\python scripts\run_web_demo.py                # tạo summary.json (7 check PASS) + server
#   -> mở http://127.0.0.1:8000/web/ ; Ctrl+C để dừng ; port bận: --port 8001
#   Demo Flow: README mục 8 (Web Demo -> mở tab http://localhost:9870 -> http://localhost:8080 -> chạy evaluate.py rồi mở http://localhost:4040)

# Web UI: http://localhost:9870 (HDFS) · :8080 (Spark Master) · :8081 (Worker) · :4040 (job đang chạy)
```
