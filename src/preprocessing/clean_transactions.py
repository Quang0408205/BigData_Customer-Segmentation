"""Preprocessing (Phase 4): làm sạch dữ liệu giao dịch bằng PySpark.

Input : raw CSV trên HDFS (hdfs_loader.read_raw_transactions)  - 8 cột STRING, tên gốc.
Output: Parquet trên HDFS (hdfs.processed_dir)                  - đã ép kiểu, đổi tên, thêm Amount.

    InvoiceNo, StockCode, Description, Quantity (int), InvoiceDate (timestamp_ntz),
    UnitPrice (double), CustomerID (string), Country, Amount (double) = Quantity * UnitPrice

Mỗi rule KHÔNG xóa âm thầm: ghi detection / reason / handling và số dòng
detected_in_raw / before / removed / after / amount_removed vào báo cáo JSON.
Thứ tự rule chỉ ảnh hưởng con số "removed" của từng rule; kết quả cuối cùng như nhau
(các filter giao hoán), nên báo cáo kèm "detected_in_raw" = số dòng vi phạm trên raw gốc.

Chạy BÊN TRONG container spark-master:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 src/preprocessing/clean_transactions.py
"""

import json
import sys
import time
from pathlib import Path

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.settings import PROJECT_ROOT, hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402
from src.ingestion.csv_reader import RAW_COLUMNS  # noqa: E402

# Tên cột ở processed layer (raw giữ tên gốc)
RENAME = {"Invoice": "InvoiceNo", "Price": "UnitPrice", "Customer ID": "CustomerID"}
ROW_COLUMNS = [RENAME.get(c, c) for c in RAW_COLUMNS]  # 8 cột (string) dùng để xác định dòng trùng
OUTPUT_COLUMNS = [
    "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
    "UnitPrice", "CustomerID", "Country", "Amount",
]
HELPER_COLUMNS = ["_qty", "_price", "_ts", "_amount"]


def prepare(raw: DataFrame, ts_format: str) -> DataFrame:
    """Đổi tên cột + thêm cột đã ép kiểu.

    Cột string gốc được giữ tới cuối để xác định dòng trùng đúng như raw.
    try_cast / try_to_timestamp: giá trị không hợp lệ -> NULL (Spark 4 bật ANSI mode,
    cast thường sẽ throw lỗi).
    """
    df = raw.select([F.col(f"`{c}`").alias(RENAME.get(c, c)) for c in RAW_COLUMNS])
    return (
        df.withColumn("_qty", F.expr("try_cast(Quantity AS INT)"))
        .withColumn("_price", F.expr("try_cast(UnitPrice AS DOUBLE)"))
        .withColumn("_ts", F.try_to_timestamp(F.col("InvoiceDate"), F.lit(ts_format)))
        # 3 chữ số thập phân: UnitPrice có tối đa 3 (vd 0.001), Quantity là số nguyên
        # -> làm tròn chỉ bỏ sai số dấu phẩy động (6.95 * 12 = 83.39999999999999)
        .withColumn("_amount", F.round(F.col("_qty") * F.col("_price"), 3))
    )


def _flag(cond: Column) -> Column:
    """Điều kiện vi phạm; NULL coi là False để filter(~cond) không âm thầm bỏ dòng."""
    return F.coalesce(cond, F.lit(False))


def _size(df: DataFrame) -> tuple[int, float]:
    r = df.agg(F.count("*").alias("n"), F.sum("_amount").alias("amount")).first()
    return r["n"], round(r["amount"] or 0.0, 2)


def remove_cancellations(df: DataFrame, prefix: str) -> tuple[DataFrame, Column, dict]:
    """Bỏ dòng hủy (InvoiceNo bắt đầu bằng prefix) + dòng mua đã bị hủy tương ứng.

    Dòng mua "khớp" một dòng hủy khi cùng CustomerID, StockCode, |Quantity|, UnitPrice
    và thời điểm mua <= lần hủy cuối cùng của khóa đó. Ghép 1-1: khóa có n dòng hủy thì
    bỏ tối đa n dòng mua khớp, ưu tiên dòng mua gần thời điểm hủy nhất.
    Dòng hủy không khớp (hủy một phần, hủy đơn trước 12/2009, hủy qua mã M...) chỉ bị bỏ dòng hủy.
    """
    is_cancel = F.col("InvoiceNo").startswith(prefix)
    key = ["CustomerID", "StockCode", "_qty_abs", "_price"]
    with_abs = df.withColumn("_qty_abs", F.abs("_qty"))
    cancels = (
        with_abs.filter(is_cancel & F.col("CustomerID").isNotNull())
        .groupBy(key)
        .agg(F.count("*").alias("_n_cancel"), F.max("_ts").alias("_last_cancel"))
    )
    eligible = _flag(
        ~is_cancel & (F.col("_qty") > 0) & F.col("_n_cancel").isNotNull() & (F.col("_ts") <= F.col("_last_cancel"))
    )
    w = Window.partitionBy(*key).orderBy(eligible.desc(), F.col("_ts").desc(), F.col("InvoiceNo").desc())
    flagged = (
        with_abs.join(cancels, key, "left")
        .withColumn("_matched", eligible & (F.row_number().over(w) <= F.col("_n_cancel")))
        .withColumn("_is_cancel", is_cancel)
        .cache()
    )
    s = flagged.agg(
        F.sum(F.col("_is_cancel").cast("int")).alias("cancel_rows"),
        F.sum((F.col("_is_cancel") & F.col("CustomerID").isNotNull()).cast("int")).alias("cancel_rows_with_customer"),
        F.sum(F.col("_matched").cast("int")).alias("matched_purchase_rows"),
        F.sum(F.when(F.col("_matched"), F.col("_amount"))).alias("matched_purchase_amount"),
        F.sum(F.when(F.col("_is_cancel"), F.col("_amount"))).alias("cancel_amount"),
    ).first().asDict()
    detail = {
        "cancel_rows_removed": s["cancel_rows"] or 0,
        "cancel_rows_with_customer": s["cancel_rows_with_customer"] or 0,
        "cancel_amount": round(s["cancel_amount"] or 0.0, 2),
        "matched_purchase_rows_removed": s["matched_purchase_rows"] or 0,
        "matched_purchase_amount_removed": round(s["matched_purchase_amount"] or 0.0, 2),
        "unmatched_cancel_rows_with_customer": (s["cancel_rows_with_customer"] or 0) - (s["matched_purchase_rows"] or 0),
    }
    kept = flagged.filter(~F.col("_is_cancel") & ~F.col("_matched")).select(df.columns)
    return kept, flagged, detail


def clean_transactions(raw: DataFrame, cfg: dict) -> tuple[DataFrame, dict]:
    """Áp dụng lần lượt các rule; trả về (DataFrame đã làm sạch, báo cáo từng rule)."""
    pp = cfg["preprocessing"]
    prefix = pp["cancel_prefix"]
    is_cancel = F.col("InvoiceNo").startswith(prefix)
    non_product = pp["non_product_stockcodes"]

    base = prepare(raw, pp["timestamp_format"]).cache()
    input_rows, input_amount = _size(base)

    # Rule lọc đơn giản: (name, điều kiện vi phạm, detection, reason, handling)
    simple = {
        "invalid_invoice_date": (
            F.col("_ts").isNull(),
            "InvoiceDate NULL hoặc không parse được theo 'yyyy-MM-dd HH:mm:ss' (try_to_timestamp -> NULL)",
            "Không có thời điểm giao dịch thì không tính được Recency",
            "Loại bỏ dòng",
        ),
        "invalid_values": (
            F.col("InvoiceNo").isNull() | F.col("StockCode").isNull() | F.col("_qty").isNull() | F.col("_price").isNull(),
            "InvoiceNo/StockCode NULL, hoặc Quantity không phải số nguyên, UnitPrice không phải số (try_cast -> NULL)",
            "Không tính được Amount / không xác định được giao dịch",
            "Loại bỏ dòng",
        ),
        "non_sale_invoices": (
            ~is_cancel & ~F.col("InvoiceNo").rlike(pp["valid_invoice_pattern"]),
            f"InvoiceNo không khớp {pp['valid_invoice_pattern']} và không phải hóa đơn hủy (vd 'A' + 6 số = Adjust bad debt)",
            "Bút toán kế toán, không phải lần mua hàng của khách",
            "Loại bỏ dòng",
        ),
        "quantity_non_positive": (
            F.col("_qty") <= 0,
            "Quantity <= 0 (sau khi đã bỏ hóa đơn hủy)",
            "Giao dịch bán hàng phải có số lượng dương; Quantity âm không thuộc hóa đơn hủy là điều chỉnh tồn kho/hàng hỏng",
            "Loại bỏ dòng",
        ),
        "unit_price_non_positive": (
            F.col("_price") <= 0,
            "UnitPrice <= 0",
            "Giá 0 là hàng tặng/điều chỉnh, giá âm là bút toán nợ xấu -> không phải doanh thu",
            "Loại bỏ dòng",
        ),
        "missing_customer_id": (
            F.col("CustomerID").isNull() | (F.trim(F.col("CustomerID")) == ""),
            "CustomerID NULL hoặc rỗng",
            "RFM tính theo khách hàng: không có CustomerID thì không gán được giao dịch cho khách nào",
            "Loại bỏ dòng (không tự điền/đoán CustomerID)",
        ),
        "non_product_stockcode": (
            F.col("StockCode").isin(non_product),
            f"StockCode thuộc danh sách tường minh trong config ({len(non_product)} mã: POST, DOT, C2, M, D, BANK CHARGES, ADJUST, TEST001, gift voucher...)",
            "Phí vận chuyển, phí ngân hàng, điều chỉnh tay, giảm giá, test, voucher không phải sản phẩm -> Monetary chỉ phản ánh tiền mua hàng",
            "Loại bỏ dòng",
        ),
    }

    # detected_in_raw: số dòng vi phạm trên dữ liệu gốc (trước mọi rule) - 1 lần quét
    raw_counts = base.agg(
        *[F.sum(_flag(c).cast("int")).alias(n) for n, (c, *_rest) in simple.items()],
        F.sum(is_cancel.cast("int")).alias("cancelled_invoices"),
    ).first().asDict()
    raw_counts["duplicate_rows"] = input_rows - base.dropDuplicates(ROW_COLUMNS).count()

    order = [
        "invalid_invoice_date", "invalid_values", "duplicate_rows", "cancelled_invoices",
        "non_sale_invoices", "quantity_non_positive", "unit_price_non_positive",
        "missing_customer_id", "non_product_stockcode",
    ]
    rules = []
    current, (n_before, amt_before) = base, (input_rows, input_amount)
    to_release = []
    for i, name in enumerate(order, start=1):
        extra = None
        if name == "duplicate_rows":
            nxt = current.dropDuplicates(ROW_COLUMNS)
            meta = (
                "Dòng giống hệt nhau ở cả 8 cột raw (dropDuplicates)",
                "Gồm 22,523 dòng do 2 sheet Excel chồng lấn 01-09/12/2010 (tính đôi doanh thu) và các dòng nhập trùng; "
                "giống tới từng phút, cùng hóa đơn, cùng sản phẩm -> không phân biệt được với lỗi ghi trùng",
                "Giữ 1 bản, bỏ các bản thừa",
            )
        elif name == "cancelled_invoices":
            nxt, flagged, extra = remove_cancellations(current, prefix)
            to_release.append(flagged)
            meta = (
                f"InvoiceNo bắt đầu bằng '{prefix}' (hóa đơn hủy, Quantity âm)",
                "Hủy không phải lần mua; dòng mua đã bị hủy cũng không phải doanh thu thật "
                "(vd 80,995 sản phẩm = 168,469.60 bị hủy sau 12 phút)",
                "Bỏ dòng hủy + dòng mua khớp chính xác (cùng CustomerID, StockCode, |Quantity|, UnitPrice, mua trước khi hủy; ghép 1-1)",
            )
        else:
            cond, *meta = simple[name]
            nxt = current.filter(~_flag(cond))
        nxt = nxt.cache()
        n_after, amt_after = _size(nxt)
        rules.append({
            "step": i,
            "rule": name,
            "detection": meta[0],
            "reason": meta[1],
            "handling": meta[2],
            "detected_in_raw": raw_counts[name],
            "records_before": n_before,
            "records_removed": n_before - n_after,
            "records_after": n_after,
            "amount_removed": round(amt_before - amt_after, 2),
            **({"detail": extra} if extra else {}),
        })
        print(f"  [{i}] {name:<24} before={n_before:>9,} removed={n_before - n_after:>8,} after={n_after:>9,}")
        if current is not base:
            current.unpersist()
        for f in to_release:
            f.unpersist()
        to_release = []
        current, n_before, amt_before = nxt, n_after, amt_after

    clean = current.select(
        "InvoiceNo", "StockCode", "Description",
        F.col("_qty").alias("Quantity"),
        F.col("_ts").alias("InvoiceDate"),
        F.col("_price").alias("UnitPrice"),
        "CustomerID", "Country",
        F.col("_amount").alias("Amount"),
    )
    report = {
        "input_rows": input_rows,
        "input_amount": input_amount,
        "output_rows": n_before,
        "output_amount": amt_before,
        "rows_removed_total": input_rows - n_before,
        "pct_rows_kept": round(100 * n_before / input_rows, 2),
        "rules": rules,
    }
    base.unpersist()
    return clean, report


def summarize(df: DataFrame) -> dict:
    """Thống kê dữ liệu sau làm sạch (để đối chiếu và cho Phase 5)."""
    r = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("CustomerID").alias("customers"),
        F.countDistinct("InvoiceNo").alias("invoices"),
        F.countDistinct("StockCode").alias("stock_codes"),
        F.countDistinct("Country").alias("countries"),
        F.min("InvoiceDate").alias("invoice_date_min"),
        F.max("InvoiceDate").alias("invoice_date_max"),
        F.min("Quantity").alias("quantity_min"),
        F.max("Quantity").alias("quantity_max"),
        F.min("UnitPrice").alias("unit_price_min"),
        F.max("UnitPrice").alias("unit_price_max"),
        F.round(F.sum("Amount"), 2).alias("amount_total"),
        F.min("Amount").alias("amount_min"),
        F.max("Amount").alias("amount_max"),
    ).first().asDict()
    r["invoice_date_min"], r["invoice_date_max"] = str(r["invoice_date_min"]), str(r["invoice_date_max"])
    r["nulls"] = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in OUTPUT_COLUMNS]
    ).first().asDict()
    return r


def main() -> int:
    from src.utils.spark_session import get_spark

    cfg = load_config()
    out_uri = hdfs_uri(cfg["hdfs"]["processed_dir"])
    report_path = PROJECT_ROOT / cfg["paths"]["output_reports"] / cfg["preprocessing"]["report_filename"]

    spark = get_spark("phase4-preprocessing")
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.perf_counter()

    raw_uri = hdfs_loader.raw_dataset_uri()
    print(f"Input : {raw_uri}")
    raw = hdfs_loader.read_raw_transactions(spark, raw_uri)

    print("Rules:")
    clean, report = clean_transactions(raw, cfg)
    clean = clean.cache()

    print(f"Output: {out_uri} (Parquet, overwrite)")
    # coalesce: gộp partition không cần shuffle -> ít file Parquet hơn (xem config)
    clean.coalesce(cfg["preprocessing"]["output_files"]).write.mode("overwrite").parquet(out_uri)
    files = [f for f in hdfs_loader.list_dir(spark, out_uri) if f["path"].endswith(".parquet")]

    report["summary"] = summarize(clean)
    report["output"] = {
        "hdfs_uri": out_uri,
        "format": "parquet",
        "parquet_files": len(files),
        "bytes": sum(f["bytes"] for f in files),
        "schema": [[f.name, f.dataType.simpleString()] for f in clean.schema.fields],
    }
    report["input"] = {"hdfs_uri": raw_uri}
    report["elapsed_seconds"] = round(time.perf_counter() - t0, 1)
    report["spark_version"] = spark.version
    clean.unpersist()
    spark.stop()

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    s = report["summary"]
    print(f"\nrows {report['input_rows']:,} -> {report['output_rows']:,} ({report['pct_rows_kept']}% kept), "
          f"customers={s['customers']:,}, invoices={s['invoices']:,}, amount={s['amount_total']:,}")
    print(f"parquet files={len(files)}, bytes={report['output']['bytes']:,}, elapsed={report['elapsed_seconds']}s")
    print(f"report -> {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
