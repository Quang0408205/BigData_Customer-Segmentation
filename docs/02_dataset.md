# 02 — Dataset

> Mọi số liệu dưới đây lấy từ lần chạy thực tế ngày 2026-09-30:
> `scripts/download_dataset.py` → `scripts/profile_dataset.py` (PySpark 4.0.4, `local[*]`).
> Kết quả đầy đủ: `output/reports/dataset_profile.json` (không commit, tạo lại bằng lệnh ở cuối file).

## 1. Nguồn dữ liệu

| Mục | Giá trị |
|---|---|
| Tên | Online Retail II |
| Nguồn | UCI Machine Learning Repository (dataset id 502) |
| Trang | https://archive.ics.uci.edu/dataset/502/online+retail+ii |
| File tải | https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip |
| Nội dung | Giao dịch của một cửa hàng bán lẻ trực tuyến tại Anh, 01/12/2009 – 09/12/2011 |

## 2. File trong `data/raw/` (không commit vào Git)

| File | Kích thước | Ghi chú |
|---|---|---|
| `online_retail_II.zip` | 45,622,418 bytes | File gốc từ UCI. SHA-256 `572e3627…08e67bfb` |
| `online_retail_II.xlsx` | 45,622,278 bytes | Giải nén từ zip, gồm 2 sheet |
| `online_retail_II.csv` | 94,268,848 bytes (~94.3 MB) | **Input của Spark**: gộp 2 sheet, giữ nguyên cột và giá trị |
| `online_retail_II.metadata.json` | — | URL, thời điểm tải, sha256, số dòng và khoảng ngày của từng sheet |

**Vì sao chuyển Excel → CSV?** Spark đọc CSV native (song song theo block), còn Excel cần
thư viện ngoài và không chia nhỏ để đọc phân tán được. Việc chuyển đổi chỉ đổi *định dạng*,
không sửa giá trị nào. Các cột định danh (`Invoice`, `StockCode`, `Customer ID`) được đọc dạng
chuỗi để không bị biến thành số (ví dụ `C489449`, `85123A`, `13085` chứ không phải `13085.0`).

| Sheet | Số dòng | InvoiceDate min | InvoiceDate max |
|---|---:|---|---|
| Year 2009-2010 | 525,461 | 2009-12-01 07:45:00 | 2010-12-09 20:01:00 |
| Year 2010-2011 | 541,910 | 2010-12-01 08:26:00 | 2011-12-09 12:50:00 |
| **Tổng CSV** | **1,067,371** | | |

## 3. Schema

Tầng raw đọc **mọi cột dạng STRING** (schema tường minh trong `src/ingestion/csv_reader.py`),
để không có giá trị nào bị Spark âm thầm đổi thành NULL. Ép kiểu thuộc về preprocessing (Phase 4).

| Cột raw | Kiểu raw | Kiểu dự kiến | Ý nghĩa | Tên thường gặp (bản Online Retail I) |
|---|---|---|---|---|
| `Invoice` | string | string | Mã hóa đơn, 6 chữ số; tiền tố `C` = hủy, `A` = điều chỉnh | InvoiceNo |
| `StockCode` | string | string | Mã sản phẩm, thường 5 chữ số (+ chữ cái biến thể) | StockCode |
| `Description` | string | string | Tên sản phẩm | Description |
| `Quantity` | string | int | Số lượng trên dòng hóa đơn (âm = trả/hủy/điều chỉnh) | Quantity |
| `InvoiceDate` | string | timestamp (không timezone) | Thời điểm lập hóa đơn | InvoiceDate |
| `Price` | string | double | Đơn giá (GBP) | UnitPrice |
| `Customer ID` | string | string | Mã khách hàng 5 chữ số | CustomerID |
| `Country` | string | string | Quốc gia của khách | Country |

- Số cột: **8**. Số dòng hỏng khi parse CSV (`_corrupt_record`): **0**.
- Ép kiểu thử (`try_cast`) trên toàn bộ dữ liệu: **0** giá trị không parse được ở `Quantity`, `Price`, `InvoiceDate`.
- **Mỗi dòng là một *dòng sản phẩm trong hóa đơn*** (transaction line), không phải một hóa đơn.

## 4. Tổng quan

| Chỉ số | Giá trị |
|---|---:|
| Records | 1,067,371 |
| Columns | 8 |
| Khoảng thời gian | 2009-12-01 07:45:00 → 2011-12-09 12:50:00 (25 tháng; tháng 12/2011 chỉ đến ngày 9) |
| Customer ID khác nhau | 5,942 |
| Invoice khác nhau | 53,628 |
| Quốc gia khác nhau | 43 |
| StockCode khác nhau | 5,305 |
| Description khác nhau | 5,698 (nhiều hơn StockCode → một mã có thể có nhiều cách ghi tên) |

## 5. Missing values

| Cột | NULL | % | Chuỗi rỗng | Chuỗi giả NULL (`nan`, `None`, …) |
|---|---:|---:|---:|---:|
| Invoice | 0 | 0.00 | 0 | 0 |
| StockCode | 0 | 0.00 | 0 | 0 |
| Description | 4,382 | 0.41 | 0 | 0 |
| Quantity | 0 | 0.00 | 0 | 0 |
| InvoiceDate | 0 | 0.00 | 0 | 0 |
| Price | 0 | 0.00 | 0 | 0 |
| **Customer ID** | **243,007** | **22.77** | 0 | 0 |
| Country | 0 | 0.00 | 0 | 0 |

- 243,007 dòng thiếu `Customer ID` thuộc **8,752** hóa đơn.
- Đây là vấn đề lớn nhất với bài toán: **không có Customer ID thì không tính được RFM** cho dòng đó.

## 6. Duplicate

So sánh **cả 8 cột giống hệt nhau**:

| Chỉ số | Giá trị |
|---|---:|
| Dòng thừa (tổng − số dòng distinct) | **34,335** (3.22%) |
| Số nhóm trùng | 32,907 |
| Tổng số dòng nằm trong các nhóm trùng | 67,242 |
| Số bản sao lớn nhất của 1 dòng | 20 |
| Số dòng distinct | 1,033,036 |

Dòng thừa tập trung bất thường ở **2010-12: 23,023** (các tháng khác 215 – 1,431).
Nguyên nhân đã được kiểm chứng bằng Spark (`sheet_overlap` trong profile):

| Kiểm tra chồng lấn 2 sheet (2010-12-01 08:26 → 2010-12-09 20:01) | Giá trị |
|---|---:|
| Dòng của sheet 2009-2010 trong khoảng này | 22,523 |
| Dòng của sheet 2010-2011 trong khoảng này | 22,523 |
| Dòng có ở cả hai sheet (`intersectAll`) | 22,523 |
| Dòng chỉ có ở một bên (`exceptAll`, cả 2 chiều) | 0 |

→ **Hai sheet chứa cùng 22,523 dòng giao dịch** của 01–09/12/2010. Đây là trùng do cách phát hành
dataset, không phải hành vi khách hàng.

→ Sau khi bỏ phần chồng lấn, vẫn còn **11,812** dòng thừa "thật" (dòng giống hệt nhau trong cùng dữ liệu,
ví dụ cùng hóa đơn, cùng sản phẩm, cùng phút). Kiểm tra: 22,523 + 11,812 = 34,335.

## 7. Giá trị số

| | min | p01 | p25 | median | p75 | p99 | max | mean |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Quantity | −80,995 | −3 | 1 | 3 | 10 | 100 | 80,995 | 9.94 |
| Price | −53,594.36 | 0.21 | 1.25 | 2.10 | 4.15 | 18.00 | 38,970.00 | 4.65 |

(percentile là xấp xỉ, `percentile_approx` với accuracy 10,000)

Phân phối **lệch phải rất mạnh**: 99% dòng có Quantity ≤ 100 và Price ≤ 18, nhưng max lên tới hàng chục nghìn.

## 8. Vấn đề dữ liệu phát hiện được (chỉ phát hiện, chưa xử lý)

| # | Vấn đề | Số lượng | Quan sát |
|---|---|---:|---|
| 1 | Thiếu Customer ID | 243,007 dòng (22.77%) | Không dùng được cho RFM |
| 2 | Dòng trùng hoàn toàn | 34,335 dòng thừa | 22,523 do chồng lấn sheet + 11,812 trùng khác |
| 3 | Hóa đơn hủy (`Invoice` bắt đầu `C`) | 19,494 dòng / 8,292 hóa đơn | 19,493 dòng có Quantity < 0; 1 dòng (C496350, `M` Manual) có Quantity = 1 |
| 4 | Quantity < 0 nhưng không phải hóa đơn hủy | 3,457 dòng | **Tất cả** không có Customer ID; thường Price = 0, Description NULL hoặc ghi chú kho ("short", "85123a mixed") → điều chỉnh tồn kho |
| 5 | Quantity = 0 | 0 | — |
| 6 | Price = 0 | 6,202 dòng | Chỉ 71 dòng có Customer ID |
| 7 | Price < 0 | 5 dòng | Tất cả là hóa đơn `A` "Adjust bad debt", StockCode `B`, không có Customer ID |
| 8 | Hóa đơn điều chỉnh (`Invoice` bắt đầu `A`) | 6 dòng / 6 hóa đơn | 5 dòng giá âm + 1 dòng giá +11,062.06 |
| 9 | StockCode không phải sản phẩm | 63 mã, 6,094 dòng | Heuristic: không khớp `^\d{5}[A-Za-z]*$`. Top: `POST` 2,122, `DOT` 1,446, `M` 1,421, `C2` 282, `D` (Discount) 177, `S` 104, `BANK CHARGES` 102, `ADJUST` 67, `AMAZONFEE` 43. Một số mã như `DCGS0058` có thể là sản phẩm thật → cần xem lại ở Phase 4 |
| 10 | Giá cực lớn | — | 6 dòng giá cao nhất đều là `M`, `BANK CHARGES`, `AMAZONFEE`, phần lớn là hóa đơn hủy (vd C556445 Price 38,970) |
| 11 | Quantity cực lớn | — | Cặp mua–hủy: 581483 / C581484 (±80,995, khách 16446), 541431 / C541433 (±74,215, khách 12346) |
| 12 | Country = "Unspecified" | 756 dòng | — |
| 13 | Khách có nhiều Country | 13 khách | Ảnh hưởng nếu phân tích theo quốc gia |
| 14 | Khoảng trắng thừa trong Description | (quan sát trong mẫu) | vd `" WHITE CHERRY LIGHTS"`, `"RECORD FRAME 7\" SINGLE SIZE "` |

Kiểm tra định dạng: 100% Customer ID (không NULL) có đúng 5 chữ số.

**Phân bố quốc gia (top 5):** United Kingdom 981,330 dòng (91.94%), EIRE 17,866, Germany 17,624,
France 14,330, Netherlands 5,140.

**Phân bố theo tháng:** trong các tháng đủ ngày, thấp nhất 27,707 (2011-02), cao nhất 84,711 (2011-11);
tháng 2011-12 chỉ có 25,526 dòng vì dữ liệu dừng ở ngày 09. Đỉnh rõ rệt vào
tháng 10–11 mỗi năm (mùa mua sắm cuối năm). Tháng 2010-12 (65,004) bị thổi phồng bởi 22,523 dòng chồng lấn.

## 9. Records mẫu

```text
Invoice StockCode Description                         Quantity InvoiceDate          Price  Customer ID Country
489434  85048     15CM CHRISTMAS GLASS BALL 20 LIGHTS 12       2009-12-01 07:45:00  6.95   13085       United Kingdom
489434  79323P    PINK CHERRY LIGHTS                  12       2009-12-01 07:45:00  6.75   13085       United Kingdom
C489449 22087     PAPER BUNTING WHITE LACE            -12      2009-12-01 10:33:00  2.95   16321       Australia
489464  21733     85123a mixed                        -96      2009-12-01 10:52:00  0.0    NULL        United Kingdom
A506401 B         Adjust bad debt                     1        2010-04-29 13:36:00  -53594.36 NULL     United Kingdom
```

## 10. Ý nghĩa cho các Phase sau (đề xuất — quyết định chính thức ở Phase 4)

- Loại dòng thiếu Customer ID (bắt buộc cho RFM) — mất 22.77% dòng.
- Loại dòng trùng hoàn toàn — nhất định phải loại phần chồng lấn sheet, nếu không doanh thu 01–09/12/2010 bị đếm đôi.
- Xử lý hóa đơn hủy / Quantity ≤ 0 / Price ≤ 0 và các mã không phải sản phẩm (POST, M, BANK CHARGES…),
  vì chúng làm sai Monetary và Frequency.
- Phân phối lệch mạnh → cần log transform + scaling trước K-Means (Phase 6).
- Recency: ngày giao dịch cuối cùng trong dữ liệu là 2011-12-09 12:50:00.

**Đã thực hiện:** Phase 4 làm sạch 1,067,371 → 770,563 dòng theo 9 rule (gồm bỏ dòng hủy + dòng mua bị hủy khớp, danh sách 25 mã
không phải sản phẩm — `DCGS*`, `SP1002` được giữ là sản phẩm thật) — `03_preprocessing.md`. Phase 6 dùng log1p + StandardScaler —
`06_kmeans.md`. AnalysisDate = 2011-12-10 — `05_rfm.md`.

## 11. Tái lập

```bash
.venv\Scripts\python scripts\download_dataset.py          # bỏ qua nếu CSV đã có; --force để tải lại
.venv\Scripts\python scripts\profile_dataset.py           # ~40 giây trên local[*]
```
