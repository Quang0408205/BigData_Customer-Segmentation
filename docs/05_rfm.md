# 05 — RFM Feature Engineering

> Phase 5 (2026-10-01). Mọi con số lấy từ lần chạy thật trên cluster
> (`output/reports/rfm_report.json`, `output/reports/rfm_check.json`).

## 1. Từ transaction-level sang customer-level

| | Transaction-level (input) | Customer-level (output) |
|---|---|---|
| 1 dòng là | 1 sản phẩm trong 1 hóa đơn | 1 khách hàng |
| Số dòng | 770,563 | **5,839** |
| Ví dụ | `489434, 22041, RECORD FRAME…, 48, 2009-12-01 07:45, 2.1, 13085, …, 100.8` | `12347, Recency 3, Frequency 8, Monetary 4921.53` |
| HDFS | `/data/customer-segmentation/processed/` | `/data/customer-segmentation/rfm/` |

**Vì sao phải aggregate?** Mục tiêu là phân khúc **khách hàng**, nên K-Means cần mỗi điểm dữ liệu là một khách.
Nếu phân cụm từng dòng giao dịch, một khách sẽ nằm rải rác trong nhiều cụm, và khách mua nhiều sẽ chiếm nhiều điểm hơn.
Aggregate (`groupBy(CustomerID).agg(...)`) gom mọi giao dịch của một khách thành một vector đặc trưng.

```text
processed (770,563 dòng)  ──groupBy(CustomerID)──►  max(InvoiceDate) · countDistinct(InvoiceNo) · sum(Amount)
                                                     ▼
                                     RFM (5,839 dòng): CustomerID | Recency | Frequency | Monetary
```

Spark làm việc này phân tán: mỗi executor tính aggregate cục bộ trên partition của mình (partial aggregation),
rồi shuffle theo `CustomerID` để gộp kết quả cuối — không cần đưa 770K dòng về một máy.

## 2. RFM là gì và vì sao hợp với clustering?

**RFM** là mô hình phân tích khách hàng kinh điển trong marketing, mô tả hành vi mua bằng 3 con số:

| Đặc trưng | Câu hỏi | Ý nghĩa |
|---|---|---|
| **R**ecency | Lần cuối khách mua cách đây bao lâu? | Nhỏ = khách vừa mua, còn "gắn bó" |
| **F**requency | Khách mua bao nhiêu lần? | Lớn = khách quay lại thường xuyên |
| **M**onetary | Khách đã chi bao nhiêu tiền? | Lớn = khách đóng góp doanh thu nhiều |

Vì sao hợp với K-Means:
- **Số liệu thật, không cần nhãn:** cả 3 tính trực tiếp từ lịch sử giao dịch. K-Means là học không giám sát nên không cần biết trước khách thuộc nhóm nào.
- **Ít chiều, dễ diễn giải:** 3 đặc trưng số liên tục → tâm cụm (centroid) đọc được ngay, ví dụ "R thấp, F cao, M cao".
- **Không trùng lặp thông tin:** 3 khía cạnh khác nhau (thời gian, tần suất, giá trị). Tương quan thật: R–F = −0.26, R–M = −0.125,
  F–M = 0.625. F và M có liên quan, nhưng không đến mức thừa.

## 3. Định nghĩa đã chốt

### AnalysisDate

**Rule:** `AnalysisDate = to_date(max(InvoiceDate) của dữ liệu processed) + 1 ngày` (config `rfm.analysis_date: null`, `analysis_date_offset_days: 1`).

- max InvoiceDate (processed) = `2011-12-09 12:50:00` → **AnalysisDate = 2011-12-10**.
- Lấy từ dữ liệu, không lấy ngày hôm nay (2026): dataset kết thúc năm 2011, nếu dùng ngày hiện tại thì mọi khách đều có Recency
  khoảng 5,400 ngày, và chênh lệch giữa các khách trở nên tương đối nhỏ.
- **+1 ngày** giống "ngày chạy phân tích ngay sau khi chốt dữ liệu": khách mua vào ngày cuối có Recency = 1, không phải 0.
- Có thể đặt ngày cố định (`rfm.analysis_date: "YYYY-MM-DD"`) nếu muốn phân tích tại thời điểm khác.

**AnalysisDate ảnh hưởng Recency thế nào:** Recency = AnalysisDate − ngày mua gần nhất. Đổi AnalysisDate thêm d ngày thì
**mọi** Recency cùng tăng d → thứ tự giữa các khách không đổi, chỉ dịch giá trị. Với StandardScaler (Phase 6), phép dịch
này bị trừ đi khi chuẩn hóa (trừ mean) → kết quả phân cụm gần như không đổi. Tuy vậy AnalysisDate phải **sau hoặc bằng**
giao dịch cuối, nếu không Recency sẽ âm.

### Recency

`Recency = datediff(AnalysisDate, to_date(max(InvoiceDate)))`, tính bằng **số ngày** (int).
So theo ngày, không theo giờ: mua lúc 8h hay 17h cùng một ngày không khác nhau về hành vi.

### Frequency

`Frequency = countDistinct(InvoiceNo)` = **số hóa đơn khác nhau** = số lần mua thực tế.
**Không** đếm số dòng giao dịch: một hóa đơn 20 sản phẩm là 20 dòng nhưng chỉ là **1 lần mua**. Đếm dòng sẽ đo
"độ rộng giỏ hàng" chứ không phải tần suất. Đã kiểm tra: mỗi InvoiceNo thuộc đúng 1 khách (0 hóa đơn có > 1 CustomerID), và
tổng Frequency = 36,338 = số hóa đơn trong processed.

### Monetary

`Monetary = round(sum(Amount), 2)` với `Amount = Quantity × UnitPrice` (Phase 4) = **tổng tiền mua sản phẩm** của khách.
Vì Phase 4 đã bỏ dòng hủy + dòng mua bị hủy, hóa đơn điều chỉnh và các mã phí, nên Monetary > 0 với mọi khách.
Tổng Monetary = 16,521,624.49 (tổng Amount processed 16,521,624.51; chênh 0.02 do làm tròn từng khách).

## 4. Thống kê (5,839 khách)

| | min | p25 | median | p75 | p90 | p99 | max | mean | std | skewness |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Recency** (ngày) | 1 | 26 | 96 | 380 | 534 | 727 | 739 | 200.91 | 208.61 | 0.89 |
| **Frequency** (hóa đơn) | 1 | 1 | 3 | 7 | 13 | 46 | 369 | 6.22 | 12.64 | **11.96** |
| **Monetary** | 2.90 | 336.09 | 851.01 | 2,207.08 | 5,321.48 | 26,924.01 | 579,128.64 | 2,829.53 | 13,900.43 | **26.61** |

Max Recency 739 = từ 2009-12-01 (ngày đầu dataset) đến AnalysisDate.

Phân bố:

| Frequency | 1 | 2 | 3–5 | 6–10 | 11–50 | > 50 |
|---|---:|---:|---:|---:|---:|---:|
| Số khách | 1,613 (27.6%) | 952 | 1,497 | 924 | 800 | 53 |

| Recency (ngày) | 1–30 | 31–90 | 91–180 | 181–365 | > 365 |
|---|---:|---:|---:|---:|---:|
| Số khách | 1,623 | 1,253 | 585 | 789 | 1,589 |

Một số khách mẫu (5 CustomerID nhỏ nhất):

| CustomerID | Recency | Frequency | Monetary |
|---|---:|---:|---:|
| 12346 | 530 | 2 | 169.36 |
| 12347 | 3 | 8 | 4,921.53 |
| 12348 | 76 | 5 | 1,658.40 |
| 12349 | 19 | 3 | 3,678.69 |
| 12350 | 311 | 1 | 294.40 |

## 5. Outlier và độ lệch (skew)

- **Lệch phải rất mạnh** ở Frequency (skew 11.96) và Monetary (skew 26.61): mean gấp 2–3 lần median
  (Monetary mean 2,829.53 so với median 851.01). Nhiều khách mua ít, một số rất ít khách mua cực nhiều.
  **Top 1% khách (58 người) chiếm 31.02% tổng Monetary**; 13 khách có Monetary > 100,000.
- **Outlier theo quy tắc IQR** (> Q3 + 1.5 × IQR), **chỉ đếm, không xóa:** Frequency > 16: **421 khách**,
  Monetary > 5,013.57: **616 khách**, Recency: 0.
- Top Monetary: 18102 (R 1, F 145, M 579,128.64) · 14646 (R 2, F 144, M 523,987.34) · 14156 · 14911 · 17450.
  Top Frequency: 14911 (F 369), 12748 (F 316), 17841 (F 211). Đây là khách sỉ/khách lớn **thật**, không phải lỗi dữ liệu → giữ lại.
- Kiểm chứng quyết định Phase 4 (bỏ dòng mua bị hủy):
  - khách **12346**: Monetary = 169.36 (nếu chỉ bỏ dòng hủy thì khoảng 77,353);
  - khách **16446**: Monetary = **2.90**, Frequency 1 (nếu chỉ bỏ dòng hủy thì khoảng 168,472). Đây là Monetary nhỏ nhất dataset:
    khách chỉ còn 1 hóa đơn nhỏ sau khi đơn 80,995 sản phẩm bị hủy.
- Outlier còn lại có thể là artefact: khách **15098** (R 183, F 2, M 39,619.50) gồm dòng 38,970.00 đã bị hủy qua mã `M`
  (Phase 4 không ghép được, xem `03_preprocessing.md` mục 9). **Giữ nguyên, ghi nhận là hạn chế.**

**Ý nghĩa cho Phase 6:** K-Means dùng khoảng cách Euclid → các giá trị cực lớn của F, M sẽ kéo centroid và có thể tạo cụm chỉ có
vài khách. Thử nghiệm nhanh: skewness sau `log1p` = Recency −0.45, Frequency 1.01, Monetary 0.24 (gần đối xứng hơn nhiều).
→ Phase 6 đã dùng **`log1p` trước `StandardScaler`**, sau khi so sánh thực nghiệm có/không log1p (`06_kmeans.md`, `07_evaluation.md`).

## 6. Output

- HDFS: `hdfs://namenode:8020/data/customer-segmentation/rfm/` — 1 file `part-00000-*.snappy.parquet`, 74,485 bytes.
- Schema: `CustomerID` string · `Recency` int · `Frequency` bigint · `Monetary` double. Lưu giá trị **gốc**, chưa scale.
- Báo cáo: `output/reports/rfm_report.json` (AnalysisDate, định nghĩa, thống kê, outlier, top, mẫu).
- Thời gian: 23.7 s (đọc processed + aggregate + thống kê + ghi), 1 executor × 4 core.

## 7. Test (kết quả thật)

`scripts/check_rfm.py` → **RESULT: PASS (11/11)**:

| Check | Kết quả |
|---|---|
| `parquet_file_count_as_config` | 1 file |
| `schema_matches` | CustomerID string, Recency int, Frequency bigint, Monetary double |
| `customer_count_matches` | 5,839 = CustomerID distinct trong processed = báo cáo Phase 4 = báo cáo RFM |
| `no_nulls`, `customer_id_unique` | 0 NULL, 0 CustomerID trùng |
| `recency_in_range`, `frequency_ge_1`, `monetary_gt_0` | Recency 1–739, Frequency 1–369, Monetary 2.90–579,128.64 |
| `recomputed_rfm_matches` | Tính lại bằng **Spark SQL** (độc lập với `build_rfm.py`), so từng khách: **0 sai lệch** |
| `frequency_total_matches` | Σ Frequency 36,338 = số cặp (CustomerID, InvoiceNo) |
| `monetary_total_matches` | Σ Monetary 16,521,624.49 ≈ Σ Amount 16,521,624.51 |

## 8. Hạn chế

1. Monetary của vài khách còn bao gồm đơn mua bị hủy mà Phase 4 không ghép được (ví dụ 15098).
2. RFM chỉ mô tả *hành vi mua*, không có thông tin sản phẩm, nhân khẩu học hay kênh bán.
3. 27.6% khách chỉ mua 1 lần → Frequency của nhóm này đều bằng 1 (nhiều điểm trùng nhau trên trục F).
4. Recency phụ thuộc AnalysisDate; dữ liệu chỉ có 25 tháng nên Recency tối đa là 739 ngày.

## 9. Cách chạy

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/feature_engineering/build_rfm.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_rfm.py
```
