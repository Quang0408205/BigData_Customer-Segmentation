# 01 — Problem Definition

> Viết ở Phase 0, cập nhật Input (Phase 1) và Result (Phase 13+14, 2026-10-01).

## Problem

Doanh nghiệp bán lẻ có rất nhiều khách hàng nhưng thường đối xử với họ như nhau: cùng một chương trình khuyến mãi, cùng một kênh chăm sóc. Điều này gây lãng phí chi phí marketing và bỏ lỡ cơ hội giữ chân khách hàng giá trị.

Câu hỏi đặt ra: **Làm sao chia khách hàng thành các nhóm có hành vi mua sắm tương đồng, chỉ dựa trên lịch sử giao dịch?**

## Context

- Dữ liệu giao dịch bán lẻ có số dòng lớn (hàng trăm nghìn đến hàng triệu dòng) và tăng liên tục theo thời gian.
- Khi dữ liệu vượt quá khả năng xử lý thoải mái của một máy đơn, cần công cụ phân tán: **HDFS** để lưu trữ, **Spark** để xử lý.
- Dữ liệu: *Online Retail II* (UCI) — giao dịch của một cửa hàng bán lẻ trực tuyến tại Anh, giai đoạn 2009–2011.

## Objective

1. Xây dựng pipeline Big Data: HDFS → Spark → Preprocessing → RFM → Scaling → K-Means.
2. Mô tả mỗi khách hàng bằng 3 chỉ số **RFM**: Recency, Frequency, Monetary.
3. Phân cụm khách hàng bằng **K-Means** và chọn số cụm dựa trên **Silhouette Score**, kích thước cụm và khả năng diễn giải.
4. Diễn giải các cụm thành nhóm khách hàng có ý nghĩa kinh doanh.
5. Trình bày kết quả bằng biểu đồ.

## Scope

**Trong phạm vi:**
- Xử lý batch (theo lô) trên dữ liệu lịch sử.
- Đặc trưng RFM; thuật toán K-Means.
- Chạy trên cluster Docker (HDFS + Spark) trên một máy cá nhân.

**Ngoài phạm vi:**
- Xử lý streaming / real-time.
- Hệ gợi ý sản phẩm, dự đoán churn có giám sát.
- Dashboard, triển khai production, xác thực người dùng.
- Benchmark với dữ liệu nhân bản (scale-up) — đã bỏ khỏi roadmap, ghi là giới hạn thực nghiệm.

## Input

- Online Retail II (UCI): **1,067,371 dòng giao dịch**, 8 cột (Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country), 01/12/2009 – 09/12/2011.
- Mỗi dòng là một sản phẩm trong một hóa đơn; 5,942 khách hàng, 53,628 hóa đơn, 43 quốc gia.
- 22.77% dòng không có Customer ID; có hóa đơn hủy, dòng trùng và giá trị bất thường. Chi tiết: `02_dataset.md`.

## Expected Output

- Bảng RFM cho từng khách hàng (Parquet trên HDFS).
- Nhãn cụm cho từng khách hàng.
- Bảng Silhouette Score theo từng giá trị K.
- Hồ sơ (profile) từng cụm và đề xuất hành động kinh doanh.
- Biểu đồ phục vụ báo cáo.

## Result (kết quả thật)

| Kỳ vọng | Kết quả |
|---|---|
| Bảng RFM | 5,839 khách — HDFS `/data/customer-segmentation/rfm/` (`05_rfm.md`) |
| Nhãn cụm | K = 4 (log1p + StandardScaler + K-Means) — HDFS `/data/customer-segmentation/output/clustering/` (`06_kmeans.md`) |
| Silhouette theo K | K = 2..6: 0.6266 / 0.5060 / **0.5325** / 0.5145 / 0.4944 (`07_evaluation.md`) |
| Profile + đề xuất | 4 nhóm: giá trị cao/trung thành (20.2% khách, 73.5% doanh thu) · nguy cơ rời bỏ · khách mới · đã rời bỏ (`08_business_analysis.md`) |
| Biểu đồ | 7 PNG Matplotlib — `output/reports/charts/` |

## Expected Value

- **Kinh doanh:** nhắm marketing đúng nhóm (giữ chân khách giá trị cao, kích hoạt lại khách sắp rời bỏ), giảm chi phí khuyến mãi dàn trải.
  Đây là giá trị *kỳ vọng*; project không có thực nghiệm đo hiệu quả chiến dịch.
- **Kỹ thuật:** pipeline lưu trữ và xử lý phân tán (HDFS + Spark), chạy lại được và tái lập kết quả; thêm node không phải sửa code.
