# 08 — Cluster Analysis + Business Analysis

> Phase 8+10 (2026-10-01). Số liệu từ lần chạy thật `src/analysis/cluster_analysis.py`
> (`output/reports/cluster_profile.{csv,json}`, HDFS `/data/customer-segmentation/output/analysis/cluster_profile/`).
> Biểu đồ: `output/reports/charts/*.png` (`src/analysis/charts.py`).
>
> Thứ tự làm: **tính profile → so sánh số liệu → (sau đó) mới đề xuất diễn giải**. Code không gán nhãn tự động;
> tên nhóm ở mục 5 là **đề xuất của người phân tích** dựa trên số liệu mục 2–4.

## 1. Input

- Kết quả phân cụm Phase 6+7: 5,839 khách, **K = 4** (log1p + StandardScaler, Silhouette 0.5325), R/F/M **gốc**.
- AnalysisDate = 2011-12-10 (Phase 5).
- Thêm 1 chỉ số **mô tả** (không dùng để phân cụm): **Tenure** = số ngày từ **lần mua đầu tiên** tới AnalysisDate,
  tính từ processed. Mục đích: phân biệt "khách mới" và "khách lâu năm" khi diễn giải.
- Số thứ tự Cluster (0–3) do K-Means gán, không mang ý nghĩa thứ hạng.

## 2. Cluster profile

| Cluster | CustomerCount | % khách | AvgRecency | AvgFrequency | AvgMonetary | % doanh thu |
|---|---:|---:|---:|---:|---:|---:|
| 0 | 1,179 | 20.19% | 28.33 | 19.12 | 10,296.27 | **73.48%** |
| 1 | 1,957 | 33.52% | 395.06 | 1.38 | 313.11 | 3.71% |
| 2 | 1,261 | 21.60% | 29.03 | 3.03 | 837.64 | 6.39% |
| 3 | 1,442 | 24.70% | 228.81 | 5.05 | 1,881.63 | 16.42% |
| **Tất cả** | **5,839** | 100% | 200.91 | 6.22 | 2,829.53 | 100% (16,521,624.49) |

Trung vị và chỉ số bổ sung (R/F/M lệch phải → **trung vị đại diện cho khách "điển hình" tốt hơn trung bình**):

| Cluster | Median Recency | Median Frequency | Median Monetary | Median Tenure | TB Tenure | % mua đúng 1 lần | Tổng Monetary |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0 | 17 | 13 | 4,877.30 | 678 | 615.97 | 0.00% | 12,139,304.98 |
| 1 | 402 | 1 | 269.09 | 440 | 442.21 | **69.09%** | 612,747.49 |
| 2 | 24 | 3 | 714.96 | **220** | 279.29 | 18.72% | 1,056,268.26 |
| 3 | 185 | 4 | 1,444.58 | 619 | 573.81 | 1.73% | 2,713,303.76 |
| Tất cả | 96 | 3 | 851.01 | 530 | 474.61 | 27.62% | 16,521,624.49 |

Kiểm tra nhất quán (`cluster_profile.json → checks`, 7/7 PASS): tổng khách 5,839, tổng Monetary khớp, % khách và % doanh thu
cộng = 100, Tenure ≥ Recency với mọi khách, đủ 4 cụm.

## 3. So sánh các cluster (median cụm ÷ median toàn bộ khách)

| Cluster | Recency | Frequency | Monetary | Đọc nhanh (chỉ mô tả số) |
|---|---:|---:|---:|---|
| 0 | 0.18× | 4.33× | 5.73× | Mua gần đây, mua nhiều lần, chi nhiều |
| 1 | 4.19× | 0.33× | 0.32× | Lâu không mua, mua ít, chi ít |
| 2 | 0.25× | 1.00× | 0.84× | Mua gần đây, tần suất/chi tiêu quanh mức điển hình |
| 3 | 1.93× | 1.33× | 1.70× | Lâu hơn điển hình chưa mua, tần suất/chi tiêu trên điển hình |

(Recency: nhỏ hơn 1× = mua gần đây hơn khách điển hình.)

## 4. Phân tích từng cluster

### Cluster 0 — 1,179 khách (20.19%)

**Data Finding**
- Recency median 17 ngày, Frequency median 13 hóa đơn, Monetary median 4,877.30 — cao nhất cả 3 trục.
- 20.19% số khách nhưng chiếm **73.48% tổng doanh thu** (12.14 triệu / 16.52 triệu).
- 0% khách mua đúng 1 lần; Tenure median 678 ngày (khách lâu năm — dataset dài 739 ngày).
- Phân phối Monetary trong cụm rất rộng (biểu đồ 06): gồm cả các khách lớn nhất dataset (18102: 579,128.64; 14911: F 369).

**Business Interpretation**
- Nhóm khách **giá trị cao, trung thành, đang hoạt động** — tương ứng nhóm thường gọi là "Champions / VIP" trong phân tích RFM.
- Doanh thu phụ thuộc mạnh vào nhóm này: mất một phần nhỏ khách ở đây ảnh hưởng doanh thu nhiều hơn mất cả cụm khác.
- Giả thuyết (chưa kiểm chứng trong project): một phần là khách sỉ/doanh nghiệp, vì Frequency và Monetary rất cao.

**Recommendation**
- Ưu tiên **giữ chân**: chương trình khách thân thiết, ưu đãi theo bậc, chăm sóc riêng cho khách lớn nhất.
- Theo dõi sớm dấu hiệu giảm hoạt động (Recency tăng vượt mức thường ngày của từng khách) để can thiệp trước khi chuyển sang nhóm giống Cluster 3.
- Không cần giảm giá đại trà cho nhóm này (họ đã mua thường xuyên) — ưu tiên dịch vụ/ưu đãi giá trị gia tăng.

### Cluster 1 — 1,957 khách (33.52%)

**Data Finding**
- Recency median **402 ngày** (lần mua cuối hơn 1 năm trước AnalysisDate), Frequency median 1, Monetary median 269.09.
- **69.09% khách chỉ mua 1 lần**. Tenure median 440 ≈ Recency median 402 → phần lớn mua lần đầu cũng là lần cuối.
- Cụm đông nhất (33.52% khách) nhưng chỉ **3.71% doanh thu**.

**Business Interpretation**
- Nhóm **đã ngừng mua / mua một lần rồi không quay lại** — tương ứng "Lost / Hibernating".
- Giá trị hiện tại thấp; khả năng quay lại không biết được từ dữ liệu (không có thông tin vì sao họ rời đi).
- Lưu ý: dữ liệu có tính mùa vụ (số dòng giao dịch cao nhất tháng 10–11 mỗi năm, vd 2010-11: 78,015 và 2011-11: 84,711
  so với 27,707 ở 2011-02 — `02_dataset.md`); một số khách R ≈ 365–400
  có thể là khách **mua theo mùa mỗi năm** chứ không hẳn đã rời bỏ — là giả thuyết, cần kiểm tra lịch sử mua theo tháng.

**Recommendation**
- Chỉ dùng kênh **chi phí thấp** để thử kích hoạt lại (email/newsletter tự động, ưu đãi quay lại), không đầu tư lớn.
- Phân tích lý do mua một lần (sản phẩm, quốc gia, thời điểm) trước khi thiết kế chiến dịch.
- Đo hiệu quả bằng nhóm đối chứng (A/B) vì tỉ lệ phản hồi của nhóm này thường thấp.

### Cluster 2 — 1,261 khách (21.60%)

**Data Finding**
- Recency median 24 ngày (gần như Cluster 0), nhưng Frequency median 3 và Monetary median 714.96 — thấp hơn Cluster 0 nhiều lần.
- **Tenure median 220 ngày** — thấp nhất (các cụm khác 440–678) → đa số bắt đầu mua trong khoảng 7 tháng gần đây.
- 18.72% mua đúng 1 lần; chiếm 6.39% doanh thu.

**Business Interpretation**
- Nhóm **khách mới / đang phát triển, đang hoạt động** — tương ứng "New / Promising customers".
- Khác Cluster 0 chủ yếu ở **thời gian gắn bó** (tenure) và mức mua tích lũy, không phải ở độ "gần đây".
  Một phần F, M thấp có thể chỉ vì họ chưa có đủ thời gian để mua nhiều.

**Recommendation**
- **Nuôi dưỡng để mua lặp lại**: onboarding, gợi ý sản phẩm liên quan, ưu đãi cho lần mua tiếp theo.
- Mục tiêu theo dõi: tỉ lệ khách của cụm này có hành vi giống Cluster 0 sau 6–12 tháng (cần dữ liệu mới để đo).

### Cluster 3 — 1,442 khách (24.70%)

**Data Finding**
- Recency median **185 ngày** (~6 tháng chưa mua), Frequency median 4, Monetary median 1,444.58 — F, M trên mức điển hình.
- Tenure median 619 ngày (lâu năm), chỉ 1.73% mua 1 lần → **đã từng mua lặp lại**.
- Chiếm 16.42% doanh thu — nhóm đóng góp lớn thứ hai.

**Business Interpretation**
- Nhóm **khách từng mua đều nhưng đang giảm/ngừng hoạt động** — tương ứng "At Risk / Need attention".
- Đây là nhóm "đáng giữ" hơn Cluster 1: đã có lịch sử mua lặp lại và giá trị cao hơn, nhưng Recency cho thấy đang xa dần.
- Cũng có thể có khách mua theo mùa (xem lưu ý ở Cluster 1).

**Recommendation**
- Chiến dịch **win-back có cá nhân hóa** (dựa trên sản phẩm từng mua), liên hệ trực tiếp với khách có Monetary cao trong cụm.
- Khảo sát lý do giảm mua (giá, sản phẩm, đối thủ, dịch vụ).
- Ưu tiên ngân sách tái kích hoạt cho cụm này trước Cluster 1.

## 5. Tóm tắt — tên nhóm đề xuất (sau khi xem số liệu)

| Cluster | Tên đề xuất | Căn cứ chính (median) | % khách | % doanh thu | Hướng hành động |
|---|---|---|---:|---:|---|
| 0 | Khách giá trị cao, trung thành (Champions) | R 17 · F 13 · M 4,877 · 0% mua 1 lần | 20.19% | 73.48% | Giữ chân, chăm sóc riêng |
| 3 | Khách có nguy cơ rời bỏ (At Risk) | R 185 · F 4 · M 1,445 · tenure 619 | 24.70% | 16.42% | Win-back cá nhân hóa |
| 2 | Khách mới / tiềm năng (New / Promising) | R 24 · F 3 · M 715 · tenure 220 | 21.60% | 6.39% | Nuôi dưỡng mua lặp lại |
| 1 | Khách đã rời bỏ / mua một lần (Lost) | R 402 · F 1 · M 269 · 69% mua 1 lần | 33.52% | 3.71% | Kích hoạt chi phí thấp |

**Lưu ý về Recommendation:** đây là **đề xuất** dựa trên mô tả hành vi trong quá khứ. Project **không có thực nghiệm**
(A/B test, dữ liệu sau can thiệp), nên **không khẳng định** các chiến lược này chắc chắn làm tăng doanh thu hay giữ chân khách.
Muốn biết hiệu quả cần triển khai thử có nhóm đối chứng và đo lại.

## 6. Biểu đồ (Phase 10)

Vẽ bằng Matplotlib trên host từ các bảng nhỏ do Spark export (host không đọc được HDFS). Mỗi biểu đồ trả lời một câu hỏi;
biểu đồ chỉ hiển thị số liệu, không tự gán tên cụm.

| File (`output/reports/charts/`) | Câu hỏi | Dữ liệu | Quan sát |
|---|---|---|---|
| `01_customer_count_by_cluster.png` | Mỗi cụm có bao nhiêu khách? | `cluster_profile.csv` | Cụm 1 lớn nhất (1,957 = 33.5%); các cụm 20–25% còn lại khá cân bằng |
| `02_avg_recency_by_cluster.png` | Các cụm khác nhau thế nào về thời gian kể từ lần mua cuối? | profile (mean + median) | Cụm 0, 2 ~28–29 ngày; cụm 3 ~229; cụm 1 ~395 (trên TB chung 200.9) |
| `03_avg_frequency_by_cluster.png` | Các cụm khác nhau thế nào về số lần mua? | profile | Cụm 0 = 19.12 (gấp ~3 lần TB chung 6.22); các cụm khác 1.4–5.1 |
| `04_avg_monetary_by_cluster.png` | Các cụm khác nhau thế nào về tổng chi tiêu? | profile | Cụm 0 mean 10,296 nhưng median 4,877 → trong cụm có vài khách rất lớn kéo mean lên |
| `05_customer_vs_revenue_share.png` | Tỉ trọng doanh thu so với tỉ trọng số khách? | profile | Cụm 0: 20.2% khách → 73.5% doanh thu; cụm 1: 33.5% khách → 3.7% |
| `06_rfm_distribution_by_cluster.png` | Phân phối R/F/M trong từng cụm, các cụm chồng lấn bao nhiêu? | `customer_clusters.csv` (5,839 khách) | Recency tách rõ cụm {0, 2} và {1, 3}; F, M (thang log) tách cụm 0 với phần còn lại; cụm 1 và 3 chồng lấn nhau một phần ở Recency |
| `07_k_vs_silhouette.png` | Vì sao chọn K = 4? | `k_evaluation.csv` | Có log1p: K = 4 cao nhất trong 3..6 (0.532), cụm nhỏ nhất 1,179 khách; không log1p: Silhouette cao nhưng cụm nhỏ nhất 2–21 khách |

Biểu đồ 02–04: cột = **trung bình** (đúng yêu cầu "Average"), chấm đen = **trung vị**, đường đứt = trung bình toàn bộ khách.
Không dùng biểu đồ 3D: với 3 đặc trưng, biểu đồ 06 (3 panel) và bảng profile đọc được chính xác hơn, 3D tĩnh khó đọc trên ảnh PNG.

## 7. Hạn chế

1. Phân khúc chỉ dựa trên **R, F, M** (hành vi mua). Không có thông tin sản phẩm, kênh, nhân khẩu học, lợi nhuận (chỉ có doanh thu).
2. Tên nhóm là **diễn giải của người phân tích**, không phải nhãn có sẵn; một khách gần ranh giới 2 cụm có thể mang đặc điểm của cả hai
   (Silhouette 0.5325 = cấu trúc vừa phải, các cụm có chồng lấn — biểu đồ 06).
3. **Mùa vụ:** số giao dịch có đỉnh tháng 10–11; Recency tính tại 2011-12-10 có thể xếp khách mua theo mùa vào nhóm "lâu không mua".
4. Monetary của vài khách còn gồm đơn bị hủy chưa ghép được (Phase 4), vd khách 15098 (M 39,619.50, nằm ở **Cluster 3**)
   → sai lệch nhỏ ở cấp khách lẻ; với median của cụm thì ảnh hưởng không đáng kể.
5. Recommendation **chưa được kiểm chứng** bằng thực nghiệm.
6. Dữ liệu 2009–2011: hành vi có thể đã thay đổi; phân khúc cần chạy lại định kỳ với dữ liệu mới.

## 8. Output

| Output | Vị trí |
|---|---|
| Profile (Parquet) | HDFS `/data/customer-segmentation/output/analysis/cluster_profile/` |
| Profile (bảng nhỏ) | `output/reports/cluster_profile.csv`, `cluster_profile.json` (gồm dòng tổng thể + checks) |
| Khách + cụm + Tenure | `output/clustering/customer_clusters.csv` (5,839 dòng) — dùng vẽ biểu đồ 06 |
| Biểu đồ | `output/reports/charts/01..07_*.png` (Matplotlib, 150 dpi) |

## 9. Cách chạy

```bash
# Spark (container): profile + export bảng nhỏ
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/analysis/cluster_analysis.py
# Host (.venv): biểu đồ PNG từ bảng nhỏ
.venv\Scripts\python -m src.analysis.charts
```
