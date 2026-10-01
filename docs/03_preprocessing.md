# 03 — Preprocessing

> Phase 4 (2026-10-01). Mọi con số trong file này lấy từ lần chạy thật trên cluster
> (`output/reports/preprocessing_report.json`, `output/reports/processed_check.json`).

## 1. Mục tiêu

Biến raw transaction (CSV, 8 cột STRING, còn lỗi) thành **dữ liệu giao dịch sạch, có kiểu dữ liệu**, sẵn sàng tính RFM.
Toàn bộ xử lý bằng **PySpark DataFrame** trên cluster (không dùng pandas).

```text
HDFS raw/online_retail_II.csv  (1,067,371 dòng, CSV, STRING)
   │  hdfs_loader.read_raw_transactions(spark)
   ▼
src/preprocessing/clean_transactions.py
   1. đổi tên cột + ép kiểu an toàn (try_cast / try_to_timestamp)
   2. 9 rule làm sạch, mỗi rule ghi detection / reason / handling / before / removed / after
   3. tạo Amount = Quantity * UnitPrice
   ▼
HDFS processed/  (Parquet, 4 file, 770,563 dòng)  +  output/reports/preprocessing_report.json
   │  scripts/check_processed_data.py — đọc lại Parquet bằng Spark, 10 check
   ▼
RESULT: PASS
```

## 2. Schema processed

| Cột | Kiểu | Từ cột raw | Ghi chú |
|---|---|---|---|
| `InvoiceNo` | string | `Invoice` | đổi tên theo chuẩn Online Retail |
| `StockCode` | string | `StockCode` | |
| `Description` | string | `Description` | giữ nguyên (không dùng cho RFM) |
| `Quantity` | int | `Quantity` | `try_cast(... AS INT)` |
| `InvoiceDate` | timestamp_ntz | `InvoiceDate` | `try_to_timestamp(..., 'yyyy-MM-dd HH:mm:ss')`, không timezone |
| `UnitPrice` | double | `Price` | đổi tên; `try_cast(... AS DOUBLE)` |
| `CustomerID` | string | `Customer ID` | đổi tên (bỏ dấu cách); giữ string vì là **mã định danh**, không phải số để tính toán |
| `Country` | string | `Country` | |
| `Amount` | double | — | **mới**: `round(Quantity × UnitPrice, 3)` |

**Vì sao `try_cast` thay vì `cast`?** Spark 4 bật ANSI mode: `cast` gặp giá trị lỗi sẽ throw và dừng cả job.
`try_cast` trả về NULL → rule phát hiện và **ghi nhận** dòng lỗi thay vì job chết hoặc lỗi bị nuốt.

### Amount là gì?

`Amount = Quantity × UnitPrice` = **giá trị tiền của một dòng giao dịch** (một sản phẩm trong một hóa đơn).
Ví dụ: hóa đơn 489434 mua 48 cái `RECORD FRAME 7" SINGLE SIZE` giá 2.1 → `Amount = 100.8`.
Ở Phase 5, **Monetary của một khách = tổng Amount** mọi dòng của khách đó.
Làm tròn 3 chữ số thập phân chỉ để bỏ sai số dấu phẩy động (`6.95 × 12 = 83.39999999999999`) — UnitPrice có tối đa
3 chữ số thập phân (vd 0.001), Quantity là số nguyên nên không mất thông tin (check `amount_ne_quantity_x_price = 0`).

## 3. Các rule làm sạch (kết quả thật)

Thứ tự áp dụng như bảng. **`detected_in_raw`** = số dòng vi phạm trên raw gốc (trước mọi rule);
**removed** = số dòng thực sự bị loại ở bước đó (thấp hơn khi rule trước đã loại một phần).
Kết quả cuối **không phụ thuộc thứ tự** (các filter giao hoán), chỉ con số removed của từng rule thay đổi.

| # | Rule | Detection | Detected in raw | Before | Removed | After | Amount removed |
|---|---|---|---:|---:|---:|---:|---:|
| 1 | `invalid_invoice_date` | `InvoiceDate` NULL / không parse được | 0 | 1,067,371 | 0 | 1,067,371 | 0.00 |
| 2 | `invalid_values` | InvoiceNo/StockCode NULL; Quantity không phải số nguyên; UnitPrice không phải số | 0 | 1,067,371 | 0 | 1,067,371 | 0.00 |
| 3 | `duplicate_rows` | giống hệt cả 8 cột raw | 34,335 | 1,067,371 | **34,335** | 1,033,036 | 431,716.87 |
| 4 | `cancelled_invoices` | `InvoiceNo` bắt đầu `C` + dòng mua khớp | 19,494 | 1,033,036 | **25,250** | 1,007,786 | −834,853.01 |
| 5 | `non_sale_invoices` | InvoiceNo không khớp `^\d{6}$` (hóa đơn `A` = Adjust bad debt) | 6 | 1,007,786 | 6 | 1,007,780 | −147,614.08 |
| 6 | `quantity_non_positive` | `Quantity <= 0` | 22,950 | 1,007,780 | 3,393 | 1,004,387 | 0.00 |
| 7 | `unit_price_non_positive` | `UnitPrice <= 0` | 6,207 | 1,004,387 | 2,621 | 1,001,766 | 0.00 |
| 8 | `missing_customer_id` | `CustomerID` NULL / rỗng | 243,007 | 1,001,766 | **228,487** | 773,279 | 3,090,394.12 |
| 9 | `non_product_stockcode` | StockCode trong danh sách 25 mã (config) | 5,912 | 773,279 | 2,716 | 770,563 | 225,982.16 |
| | **Tổng** | | | **1,067,371** | **296,808** | **770,563** (72.19%) | |

Tổng Amount: 19,287,250.57 (raw, gồm cả số âm) → **16,521,624.51** (processed).
"Amount removed" âm nghĩa là rule đó bỏ các dòng có Amount âm (dòng hủy, bút toán nợ xấu).

### Lý do và cách xử lý từng rule

| # | Reason | Handling |
|---|---|---|
| 1 | Không có thời điểm giao dịch thì không tính được Recency | Loại bỏ dòng |
| 2 | Không tính được Amount / không xác định được giao dịch | Loại bỏ dòng |
| 3 | 22,523 dòng do 2 sheet Excel chồng lấn 01–09/12/2010 (nếu giữ thì doanh thu 9 ngày bị tính đôi) + 11,812 dòng nhập trùng: giống tới từng phút, cùng hóa đơn, cùng sản phẩm, cùng số lượng → không phân biệt được với lỗi ghi trùng | Giữ 1 bản, bỏ các bản thừa (`dropDuplicates` trên 8 cột) |
| 4 | Dòng hủy không phải lần mua; dòng mua **đã bị hủy** cũng không phải doanh thu thật | Bỏ dòng hủy + dòng mua khớp chính xác (mục 4) |
| 5 | Bút toán kế toán, không phải lần mua của khách | Loại bỏ dòng |
| 6 | Bán hàng phải có số lượng dương. Quantity âm còn lại (không thuộc hóa đơn hủy) là điều chỉnh tồn kho / hàng hỏng, **đều không có CustomerID** | Loại bỏ dòng |
| 7 | Giá 0 = hàng tặng/điều chỉnh; giá âm = nợ xấu → không phải doanh thu | Loại bỏ dòng |
| 8 | RFM tính theo khách hàng; không có CustomerID thì không gán được giao dịch cho ai | Loại bỏ dòng (**không** tự điền/đoán CustomerID) |
| 9 | Phí vận chuyển, phí ngân hàng, điều chỉnh tay, giảm giá, mẫu, test, voucher không phải sản phẩm → Monetary chỉ phản ánh tiền mua hàng | Loại bỏ dòng |

Vì sao rule 6/7 removed nhỏ hơn nhiều so với detected_in_raw: 19,493/22,950 dòng Quantity âm là dòng hủy (rule 4 đã bỏ);
phần lớn dòng `Price = 0` là dòng thiếu CustomerID nhưng rule 7 đứng trước rule 8 nên vẫn đếm ở rule 7.

## 4. Hóa đơn hủy — xử lý chi tiết (rule 4)

Phát hiện khi khám phá: **2 dòng giá trị lớn nhất dataset là đơn bị hủy gần như ngay lập tức**:

| Mua | Hủy | Khách | Quantity × UnitPrice |
|---|---|---|---|
| 581483, 2011-12-09 09:15 | C581484, 09:27 | 16446 | 80,995 × 2.08 = 168,469.60 |
| 541431, 2011-01-18 10:01 | C541433, 10:17 | 12346 | 74,215 × 1.04 = 77,183.60 |

Nếu chỉ bỏ dòng hủy, hai dòng mua "ảo" này vẫn được tính vào Monetary (khách 16446 có Monetary 168k với 2 hóa đơn).
→ **Quyết định (người dùng chọn 2026-10-01): bỏ dòng hủy + dòng mua khớp.**

**Cách ghép:** dòng mua khớp một dòng hủy khi cùng `CustomerID`, `StockCode`, `|Quantity|`, `UnitPrice` và thời điểm mua
≤ lần hủy cuối cùng của khóa đó. Ghép **1-1**: khóa có n dòng hủy → bỏ tối đa n dòng mua, ưu tiên dòng mua gần lúc hủy nhất
(`Window.partitionBy(key).orderBy(InvoiceDate desc)` + `row_number() <= n`).

| Chi tiết (sau dedup) | Giá trị |
|---|---:|
| Dòng hủy bị bỏ | 19,104 (Amount −1,462,050.61) |
| … trong đó có CustomerID | 18,390 |
| Dòng mua khớp bị bỏ | **6,146** (Amount 627,197.60) |
| Dòng hủy có CustomerID nhưng không tìm thấy dòng mua khớp | 12,244 |

Sau rule 4, Quantity lớn nhất trong processed là 19,152 (dòng 80,995 đã bị loại).

## 5. Danh sách StockCode không phải sản phẩm (rule 9)

Không dùng regex (Phase 1 dùng `^\d{5}[A-Za-z]*$` làm heuristic) vì regex bắt nhầm sản phẩm thật:
`SP1002` (KID'S CHALKBOARD/EASEL), `DCGS*` (hàng bán qua eBay), `47503J ` (có khoảng trắng), `PADS` (PADS TO MATCH ALL CUSHIONS).
→ Danh sách **tường minh** 25 mã trong `configs/config.yaml: preprocessing.non_product_stockcodes`, lập từ việc liệt kê
63 mã khớp heuristic trên raw kèm Description:

`POST` (POSTAGE) · `DOT` (DOTCOM POSTAGE) · `C2` (CARRIAGE) · `M`, `m` (Manual) · `D` (Discount) · `S` (SAMPLES) ·
`B` (Adjust bad debt) · `BANK CHARGES` · `AMAZONFEE` · `ADJUST`, `ADJUST2` (Adjustment by …) · `CRUK` (CRUK Commission) ·
`TEST001`, `TEST002` (test product) · `GIFT`, `gift_0001_10` … `gift_0001_90` (gift voucher).

`C3` (1 dòng, không Description, không CustomerID) không đưa vào danh sách vì không rõ ý nghĩa — dòng này đã bị loại ở rule 8.
Detected in raw theo danh sách: 5,912 dòng (heuristic Phase 1: 6,094 — chênh 182 dòng gồm các sản phẩm thật nói trên + 1 dòng `C3`).

## 6. Quyết định đã chốt

| # | Vấn đề | Quyết định | Lý do |
|---|---|---|---|
| 1 | Tên cột | `Invoice→InvoiceNo`, `Price→UnitPrice`, `Customer ID→CustomerID` từ processed | Raw giữ tên gốc; tên chuẩn dễ đọc, không có dấu cách |
| 2 | 11,812 dòng trùng ngoài vùng chồng lấn sheet | **Loại** (giữ 1 bản) | Giống hệt 8 cột tới từng phút → không chứng minh được là giao dịch lặp hợp lệ; tác động nhỏ (≈1.1% dòng) |
| 3 | StockCode không phải sản phẩm | **Loại** theo danh sách tường minh (người dùng chọn) | Monetary = tiền mua sản phẩm |
| 4 | Hóa đơn hủy | **Bỏ dòng hủy + dòng mua khớp 1-1** (người dùng chọn) | Bỏ được giao dịch mua ảo; không tạo Monetary âm |
| 5 | Trim Description | **Không** | Description không dùng cho RFM; raw/processed giữ nguyên giá trị |
| 6 | Thứ tự rule | Như bảng mục 3, báo cáo kèm `detected_in_raw` | Kết quả cuối không đổi theo thứ tự |
| 7 | Nơi lưu báo cáo rule | `output/reports/preprocessing_report.json` (local qua bind mount) | File nhỏ, đọc được trên Windows cho docs/biểu đồ |
| 8 | Số file Parquet | `coalesce(4)` (`preprocessing.output_files`) | Không gộp → 200 file ~120 KB (24.1 MB); gộp → 4 file ~2.7 MB (11.0 MB, nén tốt hơn) |

## 7. Dữ liệu sau làm sạch

| Chỉ số | Raw (Phase 1) | Processed |
|---|---:|---:|
| Dòng | 1,067,371 | **770,563** |
| Khách hàng (CustomerID) | 5,942 | **5,839** |
| Hóa đơn | 53,628 | **36,338** |
| StockCode | 5,305 | 4,613 |
| Quốc gia | 43 | 41 |
| InvoiceDate | 2009-12-01 07:45 → 2011-12-09 12:50 | 2009-12-01 07:45 → 2011-12-09 12:50 |
| Quantity min / max | −80,995 / 80,995 | 1 / 19,152 |
| UnitPrice min / max | −53,594.36 / 38,970.00 | 0.001 / 649.50 |
| Amount min / max / tổng | — | 0.001 / 38,970.00 / **16,521,624.51** |
| NULL (mọi cột) | CustomerID 243,007; Description 4,382 | **0** |
| Dòng trùng | 34,335 | **0** |

Output HDFS: `hdfs://namenode:8020/data/customer-segmentation/processed/` — 4 file `part-0000[0-3]-*.snappy.parquet`
(≈2.75 MB mỗi file, tổng 11,033,643 bytes; `hdfs dfs -du -h` = 10.5 M), replication 1, owner `spark`.
So với raw CSV 94.3 MB: Parquet nhỏ hơn ~8.5 lần (ít dòng hơn 28% + lưu theo cột + nén Snappy).

## 8. Test (kết quả thật)

`scripts/check_processed_data.py` đọc lại Parquet từ HDFS bằng Spark → **RESULT: PASS (10/10)**:

| Check | Kết quả |
|---|---|
| `parquet_files_exist`, `parquet_file_count_as_config` | 4 file = `output_files` |
| `schema_matches` | 9 cột, đúng kiểu (int / timestamp_ntz / double / string) |
| `row_count_matches_report` | 770,563 = báo cáo preprocessing; đọc lại 4 partition |
| `rule_accounting_matches_raw` | 1,067,371 (Phase 3) − 296,808 = 770,563 |
| `no_nulls`, `no_blank_customer_id` | 0 NULL mọi cột |
| `no_duplicates` | 0 |
| `no_invalid_values` | Quantity ≤ 0: 0 · UnitPrice ≤ 0: 0 · Amount ≤ 0: 0 · InvoiceNo sai mẫu: 0 · hóa đơn `C`: 0 · mã không phải sản phẩm: 0 · Amount ≠ Quantity × UnitPrice: 0 |
| `summary_matches_report` | 5,839 khách, tổng Amount 16,521,624.51 |

Chạy preprocessing 2 lần → cùng số liệu (ghi `mode("overwrite")`, chạy lại được). Raw trên HDFS không đổi (94,268,848 bytes).
Thời gian: 58.1 s (lần 1) / 47.6 s (lần 2) cho cả 9 rule + ghi Parquet, 1 executor × 4 core (số đo 1 lần, không phải benchmark).

## 9. Hạn chế

1. **12,244 dòng hủy có CustomerID không khớp được dòng mua** (hủy một phần số lượng, hủy đơn mua trước 12/2009, hủy qua mã `M`…)
   → dòng mua gốc của chúng vẫn được tính → Monetary của vài khách có thể hơi cao.
   Ví dụ: khách 15098 mua 60 × 649.50 = **38,970.00** (dòng Amount lớn nhất còn lại) và có dòng hủy `C556445` mã `M` cùng số tiền
   3 phút sau — không khớp vì khác StockCode. Cần lưu ý khi xem outlier ở Phase 5.
2. Ghép hủy dùng "lần hủy cuối cùng của khóa" thay vì ghép thời gian từng cặp → xấp xỉ khi một khóa có nhiều lần mua/hủy xen kẽ.
3. Loại 11,812 dòng trùng có thể bỏ sót một số giao dịch lặp hợp lệ (không phân biệt được trong dữ liệu).
4. Loại 22.77% dòng thiếu CustomerID: doanh thu của khách vãng lai không có trong phân khúc.
5. Description và Country giữ nguyên (khoảng trắng thừa, 13 khách có > 1 Country) — không dùng cho RFM.

## 10. Cách chạy

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 src/preprocessing/clean_transactions.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 scripts/check_processed_data.py
docker compose exec namenode hdfs dfs -ls /data/customer-segmentation/processed
```
