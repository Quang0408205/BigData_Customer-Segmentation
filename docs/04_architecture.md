# 04 — Architecture

> Phase 2 (2026-09-30): HDFS + Spark chạy bằng Docker Compose trên 1 máy Windows (Docker Desktop, 16 CPU, 7.6 GB RAM cho Docker).
> Phase 3 (2026-10-01): ingestion `data/raw/` → HDFS raw → Spark DataFrame (mục 7).
> Phase 12 (2026-10-01): kiểm tra end-to-end toàn pipeline (mục 9).
> Mọi kết quả test ở mục 6, 7 và 9 là output thật.

## 1. Kiến trúc tổng thể

```text
 Windows host
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │  .venv (Python 3.12)  ── scripts/init_hdfs.py ──► docker compose exec ...    │
 │  Trình duyệt ── :9870 :9864 :8080 :8081 :4040 (Web UI)                        │
 │                                                                              │
 │  Docker network "customer-segmentation_default"                              │
 │  ┌───────────────── HDFS (lưu trữ) ────────────┐  ┌──── Spark (tính toán) ───┐ │
 │  │ namenode :8020  metadata: file → blocks     │  │ spark-master :7077       │ │
 │  │   volume namenode-data                      │  │  + DRIVER (spark-submit) │ │
 │  │ datanode :9866  lưu block thật              │  │ spark-worker (4 core,2g) │ │
 │  │   volume datanode-data                      │  │  └─ executor (JVM+Python)│ │
 │  └─────────────────────────────────────────────┘  └──────────────────────────┘ │
 │           ▲  hdfs://namenode:8020/...   (metadata)  │                        │
 │           └──── executor đọc/ghi block trực tiếp ◄──┘ datanode:9866          │
 │  ./ (project) được mount vào /opt/project trong các container Spark          │
 └──────────────────────────────────────────────────────────────────────────────┘
```

| Service | Image | Vai trò | Port ra host |
|---|---|---|---|
| `namenode` | `apache/hadoop:3.4.1` | HDFS NameNode — quản lý namespace và vị trí block | 9870 (Web UI) |
| `datanode` | `apache/hadoop:3.4.1` | HDFS DataNode — lưu block dữ liệu | 9864 (Web UI) |
| `spark-master` | `customer-segmentation/spark:4.0.4` | Spark Master (cluster manager) + nơi chạy driver | 8080 (UI), 7077 (RPC), 4040 (driver UI) |
| `spark-worker` | `customer-segmentation/spark:4.0.4` | Spark Worker — khởi chạy executor | 8081 (UI) |

Image Spark tự build (`docker/spark/Dockerfile`) = `apache/spark:4.0.4-java21-python3` + `pyyaml`, `python-dotenv`,
`numpy` (2.2.6, thêm ở Phase 6 vì `pyspark.ml` cần)
+ `spark-defaults.conf`. Cấu hình HDFS: `docker/hadoop/hadoop.env`.

## 2. HDFS

### HDFS là gì?
**Hadoop Distributed File System** — hệ thống file phân tán: một file lớn được **chia thành các block**
và lưu trên **nhiều máy**, nhưng người dùng vẫn thấy một cây thư mục duy nhất (`/data/customer-segmentation/raw/...`).
HDFS được thiết kế cho file lớn, ghi một lần – đọc nhiều lần, đọc tuần tự với throughput cao — đúng kiểu dữ liệu giao dịch lịch sử.

### NameNode
"Mục lục" của HDFS. Lưu **metadata**: cây thư mục, quyền, mỗi file gồm những block nào, block nằm trên DataNode nào.
NameNode **không chứa dữ liệu** của file. Metadata được lưu bền vững ở `fsimage` + `edits` trong
`dfs.namenode.name.dir` (`/data/dfs/name` trong container, trên volume `namenode-data`).
Khi mới khởi động, NameNode ở **safe mode** (chỉ đọc) cho tới khi DataNode báo cáo đủ block.

### DataNode
Nơi **lưu block thật** trên đĩa (`/data/dfs/data`, volume `datanode-data`). DataNode gửi heartbeat và
block report định kỳ cho NameNode. Client đọc/ghi dữ liệu **trực tiếp với DataNode**, không đi qua NameNode.

### HDFS block
Đơn vị lưu trữ của HDFS, mặc định **128 MB** (`dfs.blocksize=134217728`). File 300 MB → 3 block (128 + 128 + 44).
Block cuối không chiếm đủ 128 MB trên đĩa. Block lớn giúp giảm lượng metadata và giảm số lần seek.
Ví dụ thật: file mẫu 89,813 bytes = **1 block** `blk_1073741825`, nằm trên `172.21.0.3:9866`.
Dataset thật (94,268,848 bytes) cũng nằm gọn trong **1 block** (đã kiểm tra ở Phase 3, mục 7).

### Replication
Mỗi block được sao chép sang nhiều DataNode (mặc định 3) để **chịu lỗi**: một máy hỏng vẫn còn bản khác.
Project chỉ có **1 DataNode** nên đặt `dfs.replication=1` ở cả server (`hadoop.env`) và client Spark
(`spark.hadoop.dfs.replication 1` trong `spark-defaults.conf` — số bản sao do *client ghi* quyết định).
Nếu để 3, mọi block sẽ bị báo *under-replicated*. **Hạn chế:** replication=1 nghĩa là không có khả năng chịu lỗi.

## 3. Spark

### Spark là gì?
Engine xử lý dữ liệu phân tán **in-memory**. Người dùng viết phép biến đổi trên DataFrame; Spark lập
kế hoạch (Catalyst optimizer), chia thành **job → stage → task**, và chạy các task song song trên nhiều core/máy.
Các phép biến đổi là **lazy** — chỉ chạy khi có *action* (`count`, `show`, `write`...).

### Spark Master / Worker / Driver / Executor (Standalone mode)
| Thành phần | Nhiệm vụ | Trong project |
|---|---|---|
| **Master** | Cluster manager: biết có những worker nào, cấp tài nguyên cho application | `spark-master:7077` |
| **Worker** | Daemon trên mỗi máy, khởi chạy executor theo yêu cầu của master | `spark-worker`: 4 core, 2 GB |
| **Driver** | Chương trình của ta (`spark-submit script.py`): tạo SparkSession, lập kế hoạch, chia task | Chạy trong container `spark-master` |
| **Executor** | JVM (kèm Python worker khi cần) chạy task, giữ cache | 1 executor × 4 core, 1 GB |

### Spark đọc dữ liệu từ HDFS như thế nào?
1. Driver gọi Hadoop FileSystem API với `hdfs://namenode:8020/...` → hỏi NameNode: file gồm block nào, ở DataNode nào.
2. Driver chia file thành **input split** (~ kích thước block, giới hạn bởi `spark.sql.files.maxPartitionBytes` = 128 MB)
   → mỗi split là **1 partition** → **1 task**.
3. Task được gửi tới executor; executor **đọc block trực tiếp từ DataNode** (`datanode:9866`).
   Spark ưu tiên chạy task trên máy có sẵn block (**data locality**) để tránh truyền dữ liệu qua mạng.
4. Khi ghi (`df.write.parquet`), mỗi partition ghi một file `part-*.parquet`; NameNode cấp block, executor ghi vào DataNode.

Code: `src/ingestion/hdfs_loader.py` (`read_raw_transactions`, `read_parquet`, `path_exists`, `list_dir`, `delete_path`)
dùng lại `src/ingestion/csv_reader.py` — cùng một hàm đọc được cả `file://` (Phase 1) và `hdfs://`.

## 4. Vì sao project cần HDFS và Spark?

**Vì sao HDFS:**
- Một nơi lưu dữ liệu **dùng chung** cho mọi executor — executor ở máy khác không đọc được ổ `C:\` của máy dev.
- Mở rộng theo chiều ngang: thêm DataNode = thêm dung lượng và thêm băng thông đọc song song.
- Chia block cho phép Spark đọc **song song**; replication cho phép chịu lỗi (khi có nhiều node).
- Tách lưu trữ khỏi tính toán: raw → processed → rfm → output đều nằm trên HDFS, mọi bước pipeline đọc/ghi cùng một chỗ.

**Vì sao Spark:**
- Xử lý song song trên nhiều core/máy; dữ liệu không cần vừa RAM của một tiến trình Python như pandas.
- DataFrame API + SQL optimizer; đọc/ghi native CSV, Parquet, HDFS.
- Có sẵn **MLlib** (VectorAssembler, StandardScaler, KMeans, ClusteringEvaluator) — đã dùng ở Phase 6+7.
- Cùng một đoạn code chạy được `local[*]` (dev) và trên cluster (chỉ đổi master URL).

Thành thật về quy mô: dataset hiện tại 94 MB vẫn xử lý được bằng pandas. Mục tiêu là xây pipeline
**mở rộng được** (thêm DataNode / worker mà không đổi code). Project **không** benchmark dataset mở rộng
(Phase 9 đã bỏ theo roadmap 2026-10-01) → đây là giới hạn thực nghiệm.

## 5. Quyết định thiết kế

| Quyết định | Lý do |
|---|---|
| Spark **4.0.4** (client pyspark 4.0.4 = cluster 4.0.4) | Máy dev dùng JDK 21; Spark 4 hỗ trợ chính thức Java 17/21. Client và cluster phải cùng version |
| Hadoop **3.4.1** | Trùng với Hadoop client đóng gói trong Spark 4.0.4 (`hadoop-client-api-3.4.1.jar`) |
| **Driver chạy trong container** `spark-master` | NameNode trả về địa chỉ DataNode trong Docker network (172.21.x.x) mà Windows host không truy cập được; executor cũng phải kết nối ngược về driver. Để mọi thứ trong cùng network tránh được cả hai vấn đề |
| Python cluster = **3.10.12** (của image chính thức) | Driver và executor cùng image → không lệch version. Host `.venv` vẫn là 3.12 cho `local[*]` |
| `dfs.replication=1` | Chỉ 1 DataNode |
| Named volume cho NameNode/DataNode | Dữ liệu HDFS còn sau `docker compose down` (đã test); `down -v` mới xóa |
| NameNode format **1 lần** (`ENSURE_NAMENODE_DIR`) | Format lại sẽ mất toàn bộ metadata |
| Thư mục project owner `spark` (chỉ thư mục, không đệ quy vào file — sửa ở Phase 12) | Spark trong container chạy bằng user `spark` → cần quyền ghi; `hadoop` là superuser; file raw giữ `hadoop`, `rw-r--r--` |
| `TIMESTAMP_NTZ` + UTC trong `spark-defaults.conf` | Giống local (Phase 1) |
| Không dùng YARN | Spark Standalone đủ cho 1 máy, ít thành phần hơn |

### Thư mục HDFS

```text
/data/customer-segmentation/
├── raw/          CSV gốc: online_retail_II.csv (Phase 3, 94,268,848 bytes, 1 block)
├── processed/    Parquet đã làm sạch (Phase 4)
├── rfm/          Parquet RFM theo khách hàng (Phase 5)
├── output/       Kết quả phân cụm (Phase 6)
└── evaluation/   Silhouette, thí nghiệm (Phase 6+7)
/tmp/phase2-smoke/online_retail_II_sample.csv   (file test 1,000 dòng của Phase 2 — giữ lại cho spark_hdfs_smoke.py)
```

Lưu ý: `/data/dfs/...` trong container là **ổ đĩa local của container** (volume Docker), còn
`/data/customer-segmentation/...` là **đường dẫn trong HDFS** — hai namespace khác nhau.

## 6. Test Phase 2 (kết quả thật)

| # | Kiểm tra | Command | Kết quả |
|---|---|---|---|
| 1 | HDFS chạy | `hdfs dfsadmin -report` | `Live datanodes (1)`: `datanode` 172.21.0.3:9866; Safe mode OFF |
| 2 | Thư mục tồn tại | `python scripts/init_hdfs.py` | 5 thư mục, owner `spark:supergroup`, chạy lại vẫn OK |
| 3 | Upload / list | `hdfs dfs -put`, `-ls`, `fsck -files -blocks -locations` | 89,813 bytes, 1 block, replication=1, HEALTHY |
| 4 | Spark chạy | `spark-submit scripts/spark_hdfs_smoke.py` | Spark 4.0.4, 1 executor, 4 core; `sum(0..9,999,999)` đúng |
| 5 | Spark đọc HDFS | (như trên) | 8 cột STRING, 1,000 dòng, 1 partition; ghi Parquet + đọc lại 1,000 dòng |
| 6 | Bền vững | `docker compose down` → `up -d` | Thư mục + file còn nguyên, NameNode không format lại |

## 7. Phase 3 — Ingestion: Local → HDFS → Spark DataFrame

### Luồng dữ liệu

```text
data/raw/online_retail_II.csv  (Windows, giữ nguyên — không sửa, không xóa)
   │ [host] scripts/upload_to_hdfs.py
   │   1. kiểm tra file local: tồn tại, không rỗng, header đúng 8 cột; tính sha256
   │   2. docker compose cp  → /tmp/online_retail_II.csv trong container namenode
   │   3. hdfs dfs -put -f   → /data/customer-segmentation/raw/online_retail_II.csv
   │   4. xóa file tạm trong container; so size + sha256 HDFS với local; fsck
   ▼
HDFS  hdfs://namenode:8020/data/customer-segmentation/raw/online_retail_II.csv   (1 block, replication 1)
   │ [cluster] src/ingestion/hdfs_loader.read_raw_transactions(spark)
   ▼
Spark DataFrame: 8 cột STRING, 1,067,371 dòng, 4 partition
   │ [cluster] scripts/check_hdfs_ingestion.py  → output/reports/hdfs_ingestion_check.json
   ▼
(Phase 4 đọc raw qua read_raw_transactions — pipeline chính không đọc data/raw/ local nữa)
```

| File | Chạy ở đâu | Vai trò |
|---|---|---|
| `scripts/upload_to_hdfs.py` | Host (.venv) | Upload raw lên HDFS, **không upload trùng**, kiểm tra sau upload. `--force` để ghi đè |
| `src/ingestion/hdfs_loader.py` | Container Spark | `raw_dataset_uri()`, `read_raw_transactions()` (điểm vào duy nhất của raw), `block_locations()` |
| `scripts/check_hdfs_ingestion.py` | Container Spark | Kiểm tra file HDFS → schema → count → null/corrupt → sample, so với Phase 1; ghi JSON |

### Vì sao upload theo cách này?

- `hdfs` CLI chỉ có trong container namenode, và host Windows không nói chuyện trực tiếp được với DataNode
  (NameNode trả IP nội bộ 172.21.x.x) → copy file vào container rồi `hdfs dfs -put` bên trong Docker network.
- `hdfs dfs -put` ghi ra `<file>._COPYING_` rồi mới đổi tên → không bao giờ để lại file dở dang ở tên thật.
- **Không upload trùng:** nếu file đã có trên HDFS với cùng **size** và **sha256** → `SKIP`. Nếu khác → báo lỗi, chỉ ghi đè khi có `--force`
  (tránh âm thầm thay raw). Không dùng `hdfs dfs -checksum` vì đó là MD5-of-CRC32 theo block, không so được với sha256 của file local.
- File trên HDFS do user `hadoop` (superuser) tạo, quyền `rw-r--r--`. Pipeline chỉ **đọc** raw; mọi kết quả ghi vào thư mục khác.

### Kết quả thật (2026-10-01)

| Kiểm tra | Command | Kết quả |
|---|---|---|
| Upload lần 1 | `python scripts/upload_to_hdfs.py` | `not found -> upload`; size local = HDFS = **94,268,848** bytes; sha256 khớp `b0dab2a0…3138e8c`; ~24.5 s cả script |
| Block | `hdfs fsck … -files -blocks -locations` | **1 block** `blk_1073741829`, len 94,268,848, `Live_repl=1`, DataNode `172.21.0.3:9866`, **HEALTHY** |
| Upload lần 2 | `python scripts/upload_to_hdfs.py` | `identical=True` → **SKIP**, exit 0 (không upload trùng) |
| File tạm | `docker compose exec namenode ls /tmp` | Không còn `online_retail_II.csv` |
| Lỗi đầu vào | `check_local_file()` với file thiếu / rỗng / header sai | Cả 3 bị chặn với thông báo rõ |
| Spark đọc HDFS | `spark-submit scripts/check_hdfs_ingestion.py` | **RESULT: PASS** (7/7 check) |

Output của `check_hdfs_ingestion.py`:

```text
bytes=94,268,848  block_size=134,217,728  replication=1
  block 0: offset=0 length=94,268,848 datanodes=['datanode']
root
 |-- Invoice: string        |-- StockCode: string     |-- Description: string   |-- Quantity: string
 |-- InvoiceDate: string    |-- Price: string         |-- Customer ID: string   |-- Country: string
records=1,067,371  columns=8  (read+count 3.32s, includes caching)
partitions=4  defaultParallelism=4  maxPartitionBytes=134217728b  openCostInBytes=4194304b
corrupt_records=0
null: Description 4,382 · Customer ID 243,007 · các cột khác 0
+-------+---------+-----------------------------------+--------+-------------------+-----+-----------+--------------+
|Invoice|StockCode|Description                        |Quantity|InvoiceDate        |Price|Customer ID|Country       |
+-------+---------+-----------------------------------+--------+-------------------+-----+-----------+--------------+
|489434 |85048    |15CM CHRISTMAS GLASS BALL 20 LIGHTS|12      |2009-12-01 07:45:00|6.95 |13085      |United Kingdom|
|489434 |79323P   |PINK CHERRY LIGHTS                 |12      |2009-12-01 07:45:00|6.75 |13085      |United Kingdom|
|489434 |79323W   | WHITE CHERRY LIGHTS               |12      |2009-12-01 07:45:00|6.75 |13085      |United Kingdom|
|489434 |22041    |RECORD FRAME 7" SINGLE SIZE        |48      |2009-12-01 07:45:00|2.1  |13085      |United Kingdom|
|489434 |21232    |STRAWBERRY CERAMIC TRINKET BOX     |24      |2009-12-01 07:45:00|1.25 |13085      |United Kingdom|
+-------+---------+-----------------------------------+--------+-------------------+-----+-----------+--------------+
PASS hdfs_bytes_match_local_metadata · columns_match_raw · all_columns_string · record_count_match_local_metadata
PASS no_corrupt_records · nulls_match_phase1_profile · first_rows_match_local_csv
```

Số kỳ vọng **không hard-code**: lấy từ `data/raw/online_retail_II.metadata.json` (bytes, rows) và
`output/reports/dataset_profile.json` (null theo cột) của Phase 1. 5 dòng đầu so với CSV local (đọc qua bind mount, chỉ để đối chiếu).
Dòng 3 có khoảng trắng đầu `" WHITE CHERRY LIGHTS"`, dòng 4 có dấu `"` — giữ nguyên ở raw, đúng như file gốc.

### 1 block nhưng 4 partition — vì sao?

Spark không chia theo block mà theo **split size** (file CSV không nén thì chia được ở bất kỳ byte nào;
reader tự bỏ dòng bị cắt ngang ở đầu split và đọc nốt dòng cuối sang split sau):

```text
bytesPerCore = (totalBytes + numFiles × openCostInBytes) / defaultParallelism
             = (94,268,848 + 1 × 4,194,304) / 4 = 24,615,788 B  (~23.5 MB)
maxSplitBytes = min(maxPartitionBytes = 128 MB, max(openCostInBytes = 4 MB, bytesPerCore)) = 24,615,788 B
partitions    = ceil(94,268,848 / 24,615,788) = ceil(3.83) = 4       ← đo thật: 4
```

→ 4 task đọc song song, đúng bằng 4 core của executor. File mẫu 1,000 dòng ở Phase 2 chỉ có 1 partition vì quá nhỏ.

### Raw giữ CSV, processed dùng Parquet — vì sao?

- **Raw = CSV:** giữ đúng định dạng nguồn (đã đổi từ Excel ở Phase 1), đọc được bằng mắt, so sánh byte-by-byte bằng sha256 với bản local.
  Raw là "bằng chứng gốc" → không biến đổi.
- **Processed = Parquet** (từ Phase 4), vì:
  1. **Columnar:** lưu theo cột → RFM chỉ cần vài cột (CustomerID, InvoiceNo, InvoiceDate, Amount) thì chỉ đọc những cột đó (**column pruning**).
  2. **Có schema + kiểu dữ liệu** trong file: không phải parse lại chuỗi, không phải `try_cast` lại mỗi lần đọc.
  3. **Nén tốt** (Snappy mặc định; cột lặp giá trị như Country nén rất mạnh) → ít I/O hơn CSV.
  4. **Predicate pushdown:** file lưu min/max theo row group → bộ lọc (vd `InvoiceDate >= ...`) bỏ qua được cả khối dữ liệu.
  5. **Chia nhiều file `part-*`** → ghi và đọc song song theo partition; là định dạng native của Spark.
- CSV vẫn phải đọc hết mọi cột và parse text mỗi lần → phù hợp cho raw (đọc 1 lần), không phù hợp cho dữ liệu được đọc lại nhiều lần.

## 8. Vận hành

```bash
docker compose up -d                      # khởi động (lần đầu sẽ build image Spark)
docker compose ps                         # trạng thái
python scripts/init_hdfs.py               # tạo thư mục HDFS (idempotent)
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 scripts/spark_hdfs_smoke.py
python scripts/upload_to_hdfs.py          # Phase 3: raw local -> HDFS (skip nếu đã có bản giống hệt; --force để ghi đè)
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 scripts/check_hdfs_ingestion.py   # RESULT: PASS
# Các bước Phase 4 -> 10: xem mục 9 (thứ tự end-to-end) hoặc README mục 6
docker compose exec namenode hdfs dfs -ls -R /data/customer-segmentation
docker compose stop                       # dừng, giữ nguyên container + dữ liệu
docker compose down                       # xóa container, GIỮ volume HDFS
docker compose down -v                    # xóa luôn dữ liệu HDFS (cẩn thận)
```

Git Bash trên Windows tự đổi đối số dạng `/tmp/...` thành đường dẫn Windows → thêm `MSYS_NO_PATHCONV=1`
trước lệnh `docker compose exec ...` có đường dẫn HDFS. PowerShell không bị.

## 9. Phase 12 — Kiểm tra end-to-end (2026-10-01, kết quả thật)

### Luồng đầy đủ và nơi chạy

```text
[host]      data/raw/online_retail_II.csv ──upload_to_hdfs.py──► HDFS raw/          (CSV, 1,067,371 dòng)
[cluster]   clean_transactions.py  ──► HDFS processed/                (Parquet 4 file, 770,563 dòng)
[cluster]   build_rfm.py           ──► HDFS rfm/                      (Parquet, 5,839 khách)
[cluster]   evaluate.py            ──► HDFS evaluation/k_selection/   + output/evaluation/k_evaluation.{json,csv}
[cluster]   kmeans.py (K = 4)      ──► HDFS output/clustering/        (Parquet, 5,839 khách + Cluster)
[cluster]   cluster_analysis.py    ──► HDFS output/analysis/cluster_profile/ + output/reports/cluster_profile.{csv,json}
[host]      python -m src.analysis.charts ──► output/reports/charts/*.png (7 biểu đồ)
```

"cluster" = `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 <script>`.

### Lần chạy end-to-end (13 bước, tuần tự, bắt đầu 2026-10-01 07:13:09 UTC)

| # | Bước | Kết quả | Thời gian* |
|---|---|---|---:|
| 1 | `init_hdfs.py` | OK: 5 thư mục | 16.8 s |
| 2 | `spark_hdfs_smoke.py` | RESULT: PASS | 14.6 s |
| 3 | `upload_to_hdfs.py` | SKIP (HDFS đã có bản giống hệt: size + sha256) | 10.5 s |
| 4 | `check_hdfs_ingestion.py` | 1,067,371 dòng, 4 partition, RESULT: PASS (7/7) | 13.2 s |
| 5 | `clean_transactions.py` | 9 rule → 770,563 dòng, 4 file Parquet | 58.0 s |
| 6 | `check_processed_data.py` | RESULT: PASS (10/10) | 21.3 s |
| 7 | `build_rfm.py` | AnalysisDate 2011-12-10, 5,839 khách | 41.5 s |
| 8 | `check_rfm.py` | 0 sai lệch khi tính lại bằng Spark SQL, RESULT: PASS (11/11) | 17.8 s |
| 9 | `evaluate.py` | K 2..6 × 2 variant, K = 4 (log1p): Silhouette 0.5325 | 42.9 s |
| 10 | `kmeans.py` | K = 4, 27 vòng, Silhouette 0.5325 | 21.4 s |
| 11 | `check_clustering.py` | RESULT: PASS (10/10) | 15.8 s |
| 12 | `cluster_analysis.py` | profile 4 cụm, RESULT: PASS (7/7) | 15.6 s |
| 13 | `python -m src.analysis.charts` | 7 PNG | 4.7 s |
| | **Tổng** | 13/13 exit code 0 | **≈ 294 s (~4.9 phút)** |

\* Thời gian đo từ host, gồm cả khởi động `spark-submit`/JVM (~10 s mỗi lần); 1 lần đo, không phải benchmark.

### Chứng minh "chạy lại được" và kết quả tái lập

- Mọi output dẫn xuất trên HDFS là file **mới** (UUID `part-*` mới, thời gian 07:15–07:18 UTC, sau thời điểm bắt đầu).
- 20/20 file trong `output/` được tạo lại sau thời điểm bắt đầu (trừ `dataset_profile.json` của Phase 1 — input kỳ vọng, không thuộc pipeline)
  và **giống hệt** bản sao lưu trước khi chạy (JSON/CSV so nội dung, bỏ qua trường thời gian chạy; 7 PNG **giống từng byte**).
- Raw: local `data/raw/online_retail_II.csv` không đổi (94,268,848 bytes, sửa lần cuối 2026-09-30); HDFS raw giống hệt (sha256).

### Lỗi phát hiện và đã sửa

`scripts/init_hdfs.py` dùng `chown -R` / `chmod -R` trên cả `/data/customer-segmentation` → mỗi lần chạy lại, file raw
(do `hadoop` upload, `rw-r--r--`) bị đổi thành owner `spark`, `rwxr-xr-x`. Nội dung không đổi nhưng trái với nguyên tắc "raw chỉ đọc"
và với tài liệu. **Sửa:** chỉ chown/chmod các **thư mục** (base + 5 thư mục con), không đệ quy. Khôi phục file raw về
`hadoop:supergroup`, `644` (chỉ metadata). Chạy lại `init_hdfs.py` → file raw giữ nguyên; `check_hdfs_ingestion.py` → PASS
(user `spark` vẫn đọc được raw nhờ quyền `r` cho others).

### Giới hạn của lần kiểm tra

- Không xóa output cũ trước khi chạy (thao tác xóa hàng loạt trên HDFS bị chặn bởi quyền của môi trường làm việc) → bằng chứng
  "tạo lại" là thời gian sửa đổi + UUID file mới, vì mọi bước ghi `mode("overwrite")`.
- Không chạy lại `download_dataset.py` (tải lại từ UCI rất chậm, ~0.1–0.5 MB/s); bước này đã kiểm tra ở Phase 1, raw local giữ nguyên.
- Không có lệnh "một phát chạy hết": các bước chạy tuần tự theo bảng trên (roadmap không yêu cầu orchestrator;
  stub `src/pipeline.py` đã xóa ở Phase 13+14).
