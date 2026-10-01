# 14 — Viva: chuẩn bị bảo vệ

> Phase 13+14 (2026-10-01). Mọi con số lấy từ lần chạy thật — đối chiếu được trong `docs/03`–`08` và `output/reports/*.json`.
> Mỗi câu: **Q** (câu hỏi) · **A** (trả lời ngắn, nói được trong ~30 giây) · **Why** (lý do/bằng chứng nếu bị hỏi sâu).

---

## 1. Tóm tắt project trong 1 phút

"Em lấy 1.07 triệu dòng giao dịch của một cửa hàng bán lẻ online ở Anh (UCI Online Retail II), đưa lên **HDFS**,
dùng **Spark** làm sạch theo 9 rule còn 770,563 dòng, gom về **5,839 khách hàng** với 3 đặc trưng **RFM**.
Sau đó **log1p + StandardScaler** rồi **K-Means** (Spark MLlib), thử K = 2..6 và chọn **K = 4** (Silhouette 0.5325, cụm nhỏ nhất 20%).
Kết quả là 4 nhóm: khách giá trị cao (20% khách nhưng 73.5% doanh thu), khách có nguy cơ rời bỏ, khách mới, và khách đã rời bỏ.
Toàn pipeline chạy lại được và cho cùng kết quả."

```text
Dataset → HDFS → Spark → Preprocessing → RFM → Scaling → K-Means → Evaluation → Analysis → Charts
1,067,371   1 block   4 partition   770,563   5,839    log1p+z    K = 4      0.5325     4 nhóm     7 PNG
```

## 2. Cheat sheet (khái niệm → 1 câu + con số của project)

| Khái niệm | Giải thích ngắn | Trong project |
|---|---|---|
| Big Data | Dữ liệu vượt khả năng lưu/xử lý thoải mái của 1 máy (Volume, Velocity, Variety, Veracity) | 94 MB không lớn; điểm Big Data là kiến trúc phân tán + Veracity (dữ liệu bẩn) |
| HDFS | File system phân tán: chia file thành block, lưu trên nhiều máy | `/data/customer-segmentation/{raw,processed,rfm,output,evaluation}` |
| NameNode | Giữ metadata: cây thư mục, file gồm block nào, block ở DataNode nào. Không giữ dữ liệu | `namenode:8020`, UI :9870 |
| DataNode | Lưu block thật, gửi heartbeat cho NameNode; client đọc block trực tiếp từ đây | 1 DataNode `datanode:9866` |
| Block | Đơn vị lưu trữ, mặc định 128 MB | File raw 94,268,848 B = **1 block** |
| Replication | Số bản sao mỗi block để chịu lỗi (mặc định 3) | **1** (chỉ 1 DataNode) — không chịu lỗi |
| Spark | Engine xử lý phân tán in-memory; lazy, chạy khi có action | Spark 4.0.4 Standalone |
| Driver / Executor | Driver lập kế hoạch, chia task; executor chạy task song song | Driver trong `spark-master`; 1 executor × 4 core |
| Partition | Mảnh dữ liệu xử lý bởi 1 task | Raw CSV 1 block nhưng **4 partition** (split ≈ 23.5 MB) |
| PySpark DataFrame | Bảng phân tán có schema, tối ưu bởi Catalyst | Toàn bộ xử lý chính; không dùng pandas cho dataset |
| Parquet | Định dạng cột, có schema, nén tốt, đọc chọn cột | Processed 11.0 MB vs CSV 94.3 MB |
| RFM | Recency, Frequency, Monetary — hành vi mua của khách | 5,839 khách |
| StandardScaler | z = (x − mean)/std → mean 0, std 1 | Sau log1p |
| K-Means | Lặp: gán điểm vào centroid gần nhất → cập nhật centroid | K = 4, seed 42, hội tụ sau 27 vòng |
| Centroid | Tâm cụm = trung bình các điểm trong cụm | 4 centroid trong không gian 3 chiều đã scale |
| Euclidean distance | √(Σ chênh lệch²) | Lý do bắt buộc phải scale |
| Silhouette | (b − a)/max(a, b): a = cách cụm mình, b = cách cụm gần nhất khác; [−1, 1] | 0.5325 ở K = 4 |

## 3. Câu hỏi – trả lời

### Big Data / Kiến trúc

**Q1. Dataset của em có thật sự là Big Data không?**
A: Về **Volume** thì không: 1.07 triệu dòng, 94 MB, một máy với pandas vẫn xử lý được. Điểm Big Data của project là **kiến trúc**:
dữ liệu lưu trên HDFS, xử lý bằng Spark phân tán, code không đổi khi thêm node; và **Veracity** — dữ liệu bẩn cần 9 rule làm sạch.
Why: file chỉ 1 block (< 128 MB). Em không benchmark dữ liệu lớn hơn (đã bỏ khỏi phạm vi), nên không khẳng định về hiệu năng ở quy mô lớn.

**Q2. Vì sao cần HDFS nếu dữ liệu nhỏ?**
A: Để executor ở bất kỳ máy nào cũng đọc được cùng một dữ liệu (executor không đọc được ổ C: của máy em), để chia block cho đọc song song,
và để mọi tầng dữ liệu (raw → processed → rfm → kết quả) nằm chung một nơi. Khi dữ liệu tăng chỉ cần thêm DataNode.
Why: driver chạy trong container, đọc `hdfs://namenode:8020/...`; Windows host không truy cập trực tiếp được DataNode.

**Q3. Vì sao dùng Spark mà không dùng pandas?**
A: Pandas cần toàn bộ dữ liệu nằm trong RAM của một tiến trình; Spark chia dữ liệu thành partition xử lý song song trên nhiều core/máy,
đọc/ghi HDFS và Parquet native, có MLlib cho scaling và K-Means. Với 94 MB pandas vẫn chạy được — em chọn Spark để pipeline mở rộng được.
Why: em **không** đo so sánh tốc độ Spark vs pandas, nên không nói "Spark nhanh hơn".

**Q4. Vì sao không dùng database (MySQL…)?**
A: Bài toán là xử lý batch toàn bộ lịch sử giao dịch (quét hết, aggregate, ML), không phải truy vấn/cập nhật từng dòng.
HDFS + Spark phù hợp với "ghi một lần, đọc nhiều lần, quét toàn bộ"; database quan hệ một máy khó mở rộng ngang cho kiểu tải này.

**Q5. NameNode và DataNode làm gì khi Spark đọc file raw?**
A: Driver hỏi NameNode file gồm block nào, nằm ở DataNode nào; sau đó executor đọc block **trực tiếp** từ DataNode, không qua NameNode.
Why: `hdfs fsck` cho thấy 1 block `blk_1073741829`, len 94,268,848, ở DataNode 172.21.0.3:9866.

**Q6. File chỉ 1 block, vậy Spark có đọc song song không?**
A: Có. Spark chia theo **split size**, không theo block: min(128 MB, max(4 MB, (94.3 MB + 4 MB)/4 core)) ≈ 23.5 MB → **4 partition** = 4 task song song.
Why: đo thật bằng `df.rdd.getNumPartitions()` = 4.

**Q7. Replication = 1 có vấn đề gì?**
A: Mất DataNode là mất dữ liệu — không chịu lỗi. Em để 1 vì chỉ có 1 DataNode; để 3 thì mọi block bị báo under-replicated.
Môi trường thật cần nhiều DataNode và replication 3.

**Q8. Raw để CSV, processed để Parquet — vì sao?**
A: Raw giữ nguyên định dạng nguồn để kiểm chứng (so sha256 với file local). Processed dùng Parquet vì lưu theo cột (chỉ đọc cột cần),
có sẵn schema/kiểu dữ liệu, nén tốt, có predicate pushdown, chia nhiều file đọc song song.
Why: processed 770,563 dòng chỉ 11.0 MB (4 file) so với CSV raw 94.3 MB.

**Q9. Làm sao chắc dữ liệu trên HDFS giống file gốc?**
A: So **size + sha256** giữa file local và file trên HDFS; Spark đọc ra 1,067,371 dòng, số null từng cột và 5 dòng đầu khớp file gốc.
Upload lần 2 tự **SKIP** vì đã có bản giống hệt.

### Preprocessing

**Q10. Em làm sạch dữ liệu thế nào?**
A: 9 rule bằng PySpark, mỗi rule ghi detection · reason · handling · trước/bỏ/sau: ngày không hợp lệ (0), giá trị không hợp lệ (0),
dòng trùng (34,335), hóa đơn hủy + dòng mua bị hủy (25,250), hóa đơn điều chỉnh `A` (6), Quantity ≤ 0 (3,393), UnitPrice ≤ 0 (2,621),
thiếu CustomerID (228,487), mã không phải sản phẩm (2,716) → **770,563 dòng**.
Why: báo cáo `output/reports/preprocessing_report.json`; check đọc lại Parquet 10/10 PASS.

**Q11. Vì sao loại dòng thiếu CustomerID mà không điền?**
A: RFM tính theo khách; không có CustomerID thì không biết giao dịch của ai. Đoán/điền ID sẽ tạo khách giả và làm sai RFM.
Why: 243,007 dòng (22.77%) thiếu ID ở raw — đây là hạn chế: khách vãng lai không được phân khúc.

**Q12. Hóa đơn hủy xử lý thế nào? Vì sao không chỉ xóa dòng hủy?**
A: Bỏ dòng hủy **và** dòng mua khớp với nó (cùng khách, mã hàng, số lượng, giá, mua trước khi hủy; ghép 1-1).
Vì phát hiện 2 dòng lớn nhất dataset (80,995 và 74,215 sản phẩm) bị hủy sau 12–16 phút — nếu chỉ xóa dòng hủy thì khách 16446 có
Monetary 168k từ một đơn không tồn tại.
Why: sau khi xử lý, khách 16446 có Monetary 2.90, khách 12346 có 169.36. Hạn chế: 12,244 dòng hủy không ghép được (hủy một phần…).

**Q13. Dòng trùng — có thể là giao dịch hợp lệ không?**
A: 22,523 dòng chắc chắn là lỗi (2 sheet Excel chồng lấn 01–09/12/2010). 11,812 dòng còn lại giống hệt 8 cột tới từng phút —
không phân biệt được với lỗi ghi trùng, nên em giữ 1 bản. Có thể bỏ sót vài giao dịch lặp thật (≈1.1% dòng) — em ghi là hạn chế.

**Q14. Vì sao bỏ các mã như POST, M, BANK CHARGES?**
A: Đó là phí vận chuyển, nhập tay, phí ngân hàng, giảm giá, test, voucher — không phải sản phẩm. Monetary nên phản ánh tiền mua hàng.
Em dùng **danh sách 25 mã** trong config thay vì regex vì regex bắt nhầm sản phẩm thật (SP1002, DCGS*, PADS).

**Q15. Amount là gì?**
A: `Amount = Quantity × UnitPrice` = giá trị tiền của một dòng giao dịch. Monetary của khách = tổng Amount.

### RFM

**Q16. RFM được tính thế nào?**
A: `groupBy(CustomerID)`: Recency = số ngày từ ngày mua gần nhất tới AnalysisDate; Frequency = số **hóa đơn khác nhau**;
Monetary = tổng Amount. 770,563 dòng → 5,839 khách.
Why: check tính lại bằng Spark SQL độc lập, so từng khách: 0 sai lệch; tổng Frequency = 36,338 = số hóa đơn.

**Q17. AnalysisDate là gì? Vì sao là 2011-12-10?**
A: Ngày "đứng" để đo Recency. Rule: ngày giao dịch cuối của dữ liệu đã làm sạch (2011-12-09) + 1 ngày, đặt trong config.
Không dùng ngày hôm nay vì dữ liệu dừng ở 2011 — mọi khách sẽ có Recency ~5,400 ngày.
Why: đổi AnalysisDate chỉ cộng/trừ cùng một số vào mọi Recency, thứ tự khách không đổi.

**Q18. Vì sao Frequency đếm hóa đơn chứ không đếm dòng?**
A: Một hóa đơn 20 sản phẩm là 20 dòng nhưng chỉ là **1 lần mua**. Đếm dòng sẽ đo độ lớn giỏ hàng chứ không phải tần suất.

**Q19. Vì sao RFM hợp với K-Means?**
A: Không cần nhãn, chỉ 3 đặc trưng số liên tục nên centroid dễ đọc, và 3 trục mô tả 3 khía cạnh khác nhau (tương quan R–F −0.26, F–M 0.625).

### Scaling / K-Means

**Q20. Vì sao phải scaling?**
A: K-Means dùng khoảng cách Euclid. Recency tính bằng ngày (1–739), Frequency vài lần, Monetary tới 579,128 —
không scale thì Monetary lấn át, K-Means chỉ chia theo tiền. StandardScaler đưa mọi đặc trưng về mean 0, std 1.

**Q21. Vì sao thêm log1p trước StandardScaler?**
A: F và M lệch phải rất mạnh (skew 11.96, 26.61). Chỉ StandardScaler thì khách lớn nhất vẫn cách trung bình 41.5 std,
K-Means dùng cả cụm cho vài khách. Thực nghiệm: không log1p → K = 3..6 đều có cụm 2–21 khách; có log1p → cụm nhỏ nhất ≥ 7.7%.

**Q22. Vì sao chọn K-Means?**
A: Đơn giản, dễ giải thích (centroid = hồ sơ trung bình), chạy phân tán tốt trên Spark MLlib, phù hợp dữ liệu số ít chiều.
Nhược điểm: phải chọn K trước, giả định cụm dạng cầu, nhạy outlier (đã giảm bằng log1p).

**Q23. K-Means chạy thế nào và khi nào dừng?**
A: Khởi tạo K centroid (k-means||, seed 42) → gán mỗi khách vào centroid gần nhất → tính lại centroid = trung bình → lặp
tới khi centroid dịch chuyển < tol 1e-4 (hội tụ) hoặc đạt 100 vòng. K = 4 hội tụ sau **27 vòng**.

### Evaluation

**Q24. Silhouette Score là gì? 0.5325 là tốt hay xấu?**
A: Mỗi khách: a = khoảng cách TB tới khách cùng cụm, b = tới cụm gần nhất khác; s = (b − a)/max(a, b), trong [−1, 1].
0.5325 là cấu trúc **vừa phải**: các cụm tách nhau tương đối nhưng có chồng lấn — bình thường với dữ liệu khách hàng liên tục.

**Q25. K được chọn thế nào?**
A: Thử K = 2..6. Tiêu chí: (1) bỏ cách scale tạo cụm outlier, (2) cụm không quá nhỏ, (3) Silhouette cao, (4) diễn giải được.
K = 2 có Silhouette cao nhất (0.6266) nhưng chỉ chia "đang mua / không mua" — quá thô. Trong K = 3..6, K = 4 cao nhất (0.5325),
cụm nhỏ nhất 20.2%, 4 nhóm khác rõ trên cả R, F, M.

**Q26. Không log1p thì Silhouette cao hơn (0.77) — sao không dùng?**
A: Vì Silhouette cao ở đó là do cô lập 2–5 khách cực lớn thành cụm riêng — điểm tách rõ nhưng không phải phân khúc có ích.
Đây là hạn chế của Silhouette: phụ thuộc scaling và outlier, nên em luôn xem kèm kích thước cụm.

**Q27. Kết quả có tái lập được không?**
A: Có. Seed 42 cố định, mọi tham số trong config. Chạy lại toàn pipeline (13 bước) ra cùng số liệu; Silhouette tính lại từ output = 0.5325;
7 biểu đồ giống từng byte.

### Cluster analysis / Business

**Q28. Cluster có phải nhãn có sẵn trong dữ liệu không?**
A: Không. Dữ liệu không có nhãn; K-Means tự tạo cụm. Số thứ tự 0–3 không có ý nghĩa. Tên nhóm em đặt **sau khi** xem profile số liệu.

**Q29. Làm sao biết cụm có ý nghĩa?**
A: (1) Silhouette 0.5325 và kích thước cụm cân bằng; (2) 4 cụm khác nhau rõ trên median R/F/M; (3) diễn giải được:
cụm 0 (R 17, F 13, M 4,877, 73.5% doanh thu), cụm 3 (R 185, F 4), cụm 2 (R 24, F 3, khách mới — gắn bó median 220 ngày),
cụm 1 (R 402, 69% mua 1 lần). Nhưng không có ground truth để chứng minh tuyệt đối.

**Q30. Business strategy có được chứng minh không?**
A: **Không.** Đề xuất (giữ chân, win-back, nuôi dưỡng, kích hoạt chi phí thấp) dựa trên hành vi quá khứ. Muốn chứng minh cần
triển khai thử có nhóm đối chứng (A/B test) — project không có dữ liệu đó.

**Q31. Phát hiện quan trọng nhất là gì?**
A: Doanh thu tập trung: cụm 0 = 20.2% khách nhưng 73.5% doanh thu; cụm đông nhất (33.5% khách) chỉ 3.7% doanh thu.
→ Ưu tiên giữ chân cụm 0 và kéo cụm 3 (đã từng mua đều, đang xa dần) quay lại.

### Hạn chế / mở rộng

**Q32. Hạn chế của project là gì?**
A: Dữ liệu nhỏ, cluster 1 máy, replication 1, không benchmark scale-up; bỏ 22.77% dòng thiếu ID; chỉ 3 đặc trưng RFM;
K-Means 1 seed; dữ liệu 2009–2011 có tính mùa vụ; đề xuất chưa kiểm chứng.

**Q33. Nếu dữ liệu tăng 100 lần thì hệ thống thay đổi thế nào?**
A: ~9.4 GB CSV ≈ 71 block. Code **không đổi**; cần thêm DataNode (và replication 3) để đủ dung lượng/chịu lỗi, thêm worker/executor
và RAM để xử lý song song, tăng `spark.sql.shuffle.partitions` và số file output. Bước nặng nhất hiện nay là preprocessing
(dedup + window → shuffle). Đây là suy luận theo kiến trúc — em **chưa đo thực nghiệm**.

## 4. Demo script (≈ 7 phút)

Chuẩn bị trước: `docker compose up -d`, mở sẵn các tab http://localhost:9870, http://localhost:8080, thư mục `output/reports/charts/`.

| Phút | Nội dung | Thao tác / nói |
|---|---|---|
| 0:00 | Problem | README mục 1: chia khách theo hành vi mua, RFM + K-Means |
| 0:45 | Dataset | `docs/02_dataset.md`: 1,067,371 dòng, 22.77% thiếu ID, 34,335 dòng trùng, hóa đơn hủy |
| 1:30 | HDFS | `docker compose ps` → UI 9870 → Browse `/data/customer-segmentation`; `hdfs fsck .../raw/online_retail_II.csv -files -blocks -locations` → 1 block, replication 1 |
| 2:30 | Spark | UI 8080: 1 worker, 4 core; chạy `SUBMIT scripts/check_hdfs_ingestion.py` → schema, 1,067,371 dòng, 4 partition, PASS (~15 s) |
| 3:30 | Preprocessing | Mở `output/reports/preprocessing_report.json` hoặc bảng `03_preprocessing.md`: 9 rule, trước/bỏ/sau → 770,563 |
| 4:15 | RFM | `05_rfm.md`: AnalysisDate 2011-12-10, ví dụ khách 12347 (R 3, F 8, M 4,921.53) |
| 4:45 | K-Means + Evaluation | Biểu đồ `07_k_vs_silhouette.png`: vì sao log1p, vì sao K = 4 |
| 5:45 | Cluster analysis | `05_customer_vs_revenue_share.png`, `06_rfm_distribution_by_cluster.png`, bảng 4 nhóm trong `08_business_analysis.md` |
| 6:30 | Kết | Hạn chế (README mục 9) + pipeline chạy lại được (bảng 13 bước) |

Nếu được yêu cầu chạy lại một bước Spark: `kmeans.py` (~20 s) cho thấy Silhouette 0.5325 và 4 cụm.

## 5. Final checklist (kiểm tra thật ngày 2026-10-01)

| Hạng mục | Trạng thái | Bằng chứng |
|---|---|---|
| Code | ✅ | `python -m compileall src scripts` OK; không còn stub/placeholder |
| Docker | ✅ | 4 container Up, namenode/datanode healthy |
| HDFS | ✅ | 5 thư mục; raw 1 block, HEALTHY, owner hadoop `rw-r--r--` |
| Spark | ✅ | `spark_hdfs_smoke.py` PASS; Spark 4.0.4, 1 executor × 4 core |
| Dataset | ✅ | Raw local = HDFS (size + sha256); `check_hdfs_ingestion.py` PASS |
| Preprocessing | ✅ | 770,563 dòng; `check_processed_data.py` PASS |
| RFM | ✅ | 5,839 khách; `check_rfm.py` PASS (0 sai lệch) |
| K-Means | ✅ | K = 4, 27 vòng; `check_clustering.py` PASS |
| Evaluation | ✅ | `k_evaluation.json/csv`, Silhouette 0.5325 |
| Analysis + biểu đồ | ✅ | `cluster_analysis.py` PASS; 7 PNG |
| End-to-end | ✅ | 13/13 bước, kết quả tái lập (`04_architecture.md` mục 9) |
| Documentation | ✅ | README + `docs/01`–`08`, `PROJECT_EXPLANATION.md`, file này |
| Benchmark scale-up | ❌ (đã bỏ) | Ghi là giới hạn thực nghiệm |
| Dashboard | ❌ (đã bỏ) | Biểu đồ PNG thay thế |
