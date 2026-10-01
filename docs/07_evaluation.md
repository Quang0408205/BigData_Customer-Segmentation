# 07 — Evaluation + chọn K

> Phase 6+7 (2026-10-01). Số liệu từ lần chạy thật `src/clustering/evaluate.py`
> (`output/evaluation/k_evaluation.json`, `.csv`; HDFS `/data/customer-segmentation/evaluation/k_selection/`).
> Model, scaling, K-Means: `docs/06_kmeans.md`.

## 1. Metric và vì sao dùng

| Metric | Đo cái gì | Vì sao cần |
|---|---|---|
| **Silhouette Score** | Mỗi khách: `s = (b − a) / max(a, b)`; a = khoảng cách TB tới khách **cùng cụm**, b = khoảng cách TB tới **cụm gần nhất khác**. Lấy trung bình mọi khách. Miền [−1, 1] | Metric chính: cụm có chặt và tách nhau không. ~1: tách rõ; ~0: nằm ở ranh giới; < 0: có thể bị gán nhầm cụm |
| **Cluster size** | Số khách mỗi cụm, % cụm nhỏ nhất | Cụm vài khách không dùng được cho chiến lược và không ổn định |
| **Processing time** | Thời gian fit + transform + tính Silhouette cho mỗi K | Yêu cầu đề bài; cho thấy chi phí khi K tăng |
| WSSSE *(bổ sung)* | Tổng bình phương khoảng cách điểm → centroid (`model.summary.trainingCost`) | Hàm mục tiêu của K-Means; dùng nhìn "elbow". **Luôn giảm khi K tăng** → không dùng một mình để chọn K |
| numIter *(bổ sung)* | Số vòng lặp thật | Xác nhận K-Means **hội tụ** (numIter < maxIter = 100), kết quả không bị cắt giữa chừng |
| TB R/F/M gốc theo cụm *(bổ sung)* | Trung bình Recency/Frequency/Monetary mỗi cụm | Đánh giá **khả năng diễn giải** — tiêu chí chọn K theo đề bài. Không đặt tên cụm ở bước này |

Silhouette của Spark (`ClusteringEvaluator`) mặc định dùng **squared Euclidean** distance (khác scikit-learn dùng Euclidean)
→ giá trị không so trực tiếp được với số của sklearn, nhưng so giữa các K trong project là nhất quán.

## 2. Kết quả (seed 42, 5,839 khách)

### Phương án chọn: log1p + StandardScaler

| K | Silhouette | Processing Time | Cluster Distribution (khách, giảm dần) | Cụm nhỏ nhất | WSSSE | numIter |
|---|---:|---:|---|---:|---:|---:|
| 2 | **0.6266** | 1.65 s | 3,515 / 2,324 | 39.80% | 8,524.8 | 12 |
| 3 | 0.5060 | 2.42 s | 2,341 / 2,256 / 1,242 | 21.27% | 6,294.8 | 32 |
| **4** | **0.5325** | 2.20 s | 1,957 / 1,442 / 1,261 / 1,179 | **20.19%** | 4,859.1 | 27 |
| 5 | 0.5145 | 2.92 s | 1,671 / 1,328 / 1,266 / 1,122 / 452 | 7.74% | 4,038.4 | 51 |
| 6 | 0.4944 | 4.05 s | 1,478 / 1,237 / 1,026 / 916 / 731 / 451 | 7.72% | 3,512.2 | 81 |

### Phương án đối chứng: StandardScaler trên giá trị gốc (không log1p)

| K | Silhouette | Processing Time | Cluster Distribution | Cụm nhỏ nhất | WSSSE | numIter |
|---|---:|---:|---|---:|---:|---:|
| 2 | 0.4146 | 4.03 s* | 3,811 / 2,028 | 34.73% | 12,446.2 | 10 |
| 3 | 0.6780 | 1.63 s | 3,828 / 1,999 / **12** | 0.21% | 7,238.0 | 7 |
| 4 | 0.5549 | 1.44 s | 3,450 / 1,537 / 831 / **21** | 0.36% | 6,432.6 | 8 |
| 5 | 0.7542 | 1.68 s | 3,812 / 1,974 / 48 / **3** / **2** | 0.03% | 4,500.9 | 13 |
| 6 | 0.7728 | 2.22 s | 3,560 / 1,906 / 348 / 18 / **5** / **2** | 0.03% | 3,365.1 | 24 |

\* K = 2 chạy đầu tiên, gồm cả thời gian khởi động (JVM warm-up, cache) → không so sánh được trực tiếp.
Thời gian là **1 lần đo** trên 1 executor × 4 core, không phải benchmark. Toàn bộ evaluate (10 model + thống kê + ghi) mất 37.1 s.

WSSSE giữa 2 phương án **không so được** với nhau (2 không gian đặc trưng khác nhau). Silhouette cũng vậy.

### Trung bình R/F/M gốc theo cụm (log1p) — dùng để đánh giá khả năng diễn giải

| K | Cụm (khách: TB Recency / TB Frequency / TB Monetary) |
|---|---|
| 2 | 3,515: 300 / 2.1 / 595 · 2,324: 51 / 12.5 / 6,209 |
| 3 | 2,341: 379 / 1.5 / 391 · 2,256: 106 / 4.2 / 1,408 · 1,242: 37 / 18.7 / 10,008 |
| **4** | 1,957: 395 / 1.4 / 313 · 1,442: 229 / 5.1 / 1,882 · 1,261: 29 / 3.0 / 838 · 1,179: 28 / 19.1 / 10,296 |
| 5 | 1,671: 421 / 1.3 / 275 · 1,328: 278 / 4.1 / 1,538 · 1,266: 40 / 8.8 / 3,313 · 1,122: 35 / 2.3 / 627 · 452: 23 / 32.9 / 20,180 |
| 6 | 1,478: 433 / 1.2 / 244 · 1,237: 311 / 3.3 / 1,155 · 1,026: 41 / 2.1 / 552 · 916: 98 / 9.4 / 3,843 · 731: 13 / 6.9 / 2,194 · 451: 16 / 32.5 / 20,046 |

Đối chứng không log1p, K = 6: các cụm nhỏ là 2 khách (TB Monetary 551,558), 5 khách (230,925), 18 khách (72,080) — tức là
K-Means dùng cụm để **cô lập từng khách lớn**, không phải để tìm nhóm hành vi.

## 3. Tiêu chí chọn K

Đặt ra trước khi chọn, áp dụng theo thứ tự:

1. **Scaling:** loại phương án mà Silhouette cao chỉ vì tách outlier. Phương án không log1p có Silhouette cao nhất (0.7728 ở K = 6),
   nhưng ở mọi K = 3..6 đều có cụm 2–21 khách (≤ 0.36%) → **loại**. Dùng **log1p + StandardScaler**.
2. **Cluster size:** mọi cụm phải đủ lớn để có ý nghĩa thống kê và hành động (với log1p, K = 2..6 đều đạt: cụm nhỏ nhất ≥ 7.7%).
3. **Silhouette:** ưu tiên K có Silhouette cao.
4. **Khả năng diễn giải:** các cụm phải khác nhau rõ trên R/F/M và mang thêm thông tin so với K nhỏ hơn.

## 4. K được sử dụng: **K = 4** (`clustering.selected_k: 4`)

Lý do:
- **K = 2 có Silhouette cao nhất (0.6266)** nhưng chỉ chia khách thành 2 nhóm "R thấp – F, M cao" và "R cao – F, M thấp" —
  gần như một trục "đang hoạt động / không hoạt động". Quá thô để phân biệt hành vi. Silhouette thường cao nhất ở K nhỏ
  (xem mục 5), nên đây không phải lý do đủ để chọn K = 2.
- Trong K = 3..6, **K = 4 có Silhouette cao nhất (0.5325)**, cao hơn K = 3 (0.5060), K = 5 (0.5145), K = 6 (0.4944).
- **Cụm cân bằng:** nhỏ nhất 1,179 khách (20.19%), lớn nhất 1,957 (33.5%).
- **Diễn giải được:** so với K = 3, K = 4 tách nhóm "mua gần đây" thành 2 nhóm khác hẳn về F và M:
  (R 29, F 3.0, M 838) và (R 28, F 19.1, M 10,296). Thêm vào đó có một nhóm R trung bình (229 ngày). Bốn nhóm khác nhau trên cả 3 trục.
  K = 5, 6 tạo thêm cụm nhưng Silhouette giảm và số vòng lặp tăng (51, 81).
- **Hội tụ:** 27 vòng. Model cuối (`kmeans.py`) tái lập đúng Silhouette 0.5325 và kích thước cụm của lần evaluate.

Không gọi K = 4 là "tốt nhất": đây là lựa chọn cân bằng giữa Silhouette, kích thước cụm và khả năng diễn giải theo tiêu chí ở mục 3.
Đổi K chỉ cần sửa `clustering.selected_k` và chạy lại `kmeans.py`.

## 5. Hạn chế của metric

1. **Silhouette ưu tiên cụm lồi, dạng cầu, tách bạch** — đúng giả định của K-Means. Dữ liệu khách hàng thường là một khối
   liên tục (không có ranh giới tự nhiên), nên Silhouette ~0.5 là mức "cấu trúc vừa phải", không chứng minh các nhóm tồn tại thật.
2. **Silhouette thường cao nhất ở K nhỏ** (K = 2) vì ít ranh giới hơn → không chọn K chỉ theo giá trị lớn nhất.
3. **Phụ thuộc scaling và outlier:** cùng dữ liệu, không log1p cho Silhouette 0.77 với cụm 2 khách. Metric cao không đồng nghĩa với phân khúc hữu ích.
4. **Tính trên không gian đã scale** (log1p + z-score), không phải trên giá trị tiền/ngày gốc.
5. **WSSSE luôn giảm khi K tăng** → chỉ dùng để tham khảo elbow. Ở đây không có "khuỷu" rõ (8,525 → 6,295 → 4,859 → 4,038 → 3,512).
6. **1 seed, 1 lần chạy:** K-Means có thể rơi vào cực tiểu cục bộ khác với seed khác. Project cố định seed 42 để tái lập;
   chưa đo độ ổn định qua nhiều seed.
7. **Không có ground truth:** không có nhãn "đúng" để so → đánh giá nội tại (Silhouette) + diễn giải nghiệp vụ (Phase 8+10).
   Cluster là kết quả của thuật toán, không phải nhãn có sẵn trong dữ liệu.

## 6. Output

- Local (cho biểu đồ Phase 8+10): `output/evaluation/k_evaluation.json` (đầy đủ, gồm TB R/F/M theo cụm) và `k_evaluation.csv`
  (`variant, k, silhouette, min_cluster_size, min_cluster_pct, cluster_sizes, wssse, num_iter, seconds`).
- HDFS: `/data/customer-segmentation/evaluation/k_selection/` (Parquet, cùng bảng CSV).
