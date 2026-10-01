"""Profile dataset raw Online Retail II bằng PySpark.

Chỉ QUAN SÁT dữ liệu (đếm, thống kê, lấy mẫu) - không làm sạch, không ghi đè raw.
Kết quả in ra console và lưu vào output/reports/dataset_profile.json
để tài liệu (docs/02_dataset.md) dựa trên số liệu thật.

Usage:
    python scripts/profile_dataset.py
    python scripts/profile_dataset.py --path hdfs://namenode:8020/...   (chạy bằng spark-submit trong container)
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, functions as F  # noqa: E402

from src.config.settings import PROJECT_ROOT, load_config  # noqa: E402
from src.ingestion.csv_reader import CORRUPT_COL, RAW_COLUMNS, read_raw_csv  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
PLACEHOLDER_STRINGS = ["nan", "NaN", "None", "NULL", "null", "N/A"]


def rows_to_dicts(rows) -> list[dict]:
    return [r.asDict() for r in rows]


def add_typed_columns(df: DataFrame) -> DataFrame:
    """Thêm cột đã ép kiểu (try_*: giá trị không hợp lệ -> NULL thay vì lỗi,
    vì Spark 4 bật ANSI mode mặc định)."""
    return (
        df.withColumn("quantity_t", F.expr("try_cast(Quantity AS INT)"))
        .withColumn("price_t", F.expr("try_cast(Price AS DOUBLE)"))
        .withColumn("date_t", F.try_to_timestamp(F.col("InvoiceDate"), F.lit(TS_FORMAT)))
    )


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def profile(df: DataFrame, show) -> dict:
    report: dict = {}

    # ---- Size & schema -----------------------------------------------------
    total = df.count()
    report["records"] = total
    report["columns"] = len(RAW_COLUMNS)
    report["raw_schema"] = [(f.name, f.dataType.simpleString()) for f in df.schema if f.name in RAW_COLUMNS]
    report["corrupt_records"] = df.filter(F.col(CORRUPT_COL).isNotNull()).count()
    section("SIZE & SCHEMA")
    print(f"records={total:,}  columns={len(RAW_COLUMNS)}  corrupt_records={report['corrupt_records']}")
    df.select(RAW_COLUMNS).printSchema()

    # ---- Missing values ----------------------------------------------------
    agg = []
    for c in RAW_COLUMNS:
        agg.append(F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(f"{c}|null"))
        agg.append(F.sum((F.trim(F.col(f"`{c}`")) == "").cast("int")).alias(f"{c}|blank"))
        agg.append(F.sum(F.col(f"`{c}`").isin(PLACEHOLDER_STRINGS).cast("int")).alias(f"{c}|placeholder"))
    m = df.agg(*agg).first().asDict()
    report["missing"] = {
        c: {
            "null": m[f"{c}|null"] or 0,
            "blank": m[f"{c}|blank"] or 0,
            "placeholder_string": m[f"{c}|placeholder"] or 0,
            "pct_null": round(100 * (m[f"{c}|null"] or 0) / total, 2),
        }
        for c in RAW_COLUMNS
    }
    section("MISSING VALUES")
    for c, v in report["missing"].items():
        print(f"{c:<12} null={v['null']:>8,} ({v['pct_null']:5.2f}%)  blank={v['blank']}  placeholder={v['placeholder_string']}")

    # ---- Type parsing ------------------------------------------------------
    report["unparseable"] = {
        "Quantity": df.filter(F.col("Quantity").isNotNull() & F.col("quantity_t").isNull()).count(),
        "Price": df.filter(F.col("Price").isNotNull() & F.col("price_t").isNull()).count(),
        "InvoiceDate": df.filter(F.col("InvoiceDate").isNotNull() & F.col("date_t").isNull()).count(),
    }
    section("TYPE PARSING (non-null raw value that cannot be cast)")
    print(report["unparseable"])

    # ---- Duplicates --------------------------------------------------------
    dup_groups = (
        df.groupBy([F.col(f"`{c}`") for c in RAW_COLUMNS]).count()
        .filter(F.col("count") > 1).cache()
    )
    d = dup_groups.agg(
        F.count("*").alias("groups"),
        F.sum("count").alias("rows_in_groups"),
        F.sum(F.col("count") - 1).alias("excess"),
        F.max("count").alias("max_copies"),
    ).first()
    distinct_rows = total - (d["excess"] or 0)
    report["duplicates"] = {
        "exact_duplicate_rows_excess": d["excess"] or 0,
        "duplicate_groups": d["groups"],
        "rows_in_duplicate_groups": d["rows_in_groups"] or 0,
        "max_copies_of_one_row": d["max_copies"],
        "distinct_rows": distinct_rows,
        "pct_excess": round(100 * (d["excess"] or 0) / total, 2),
    }
    by_month = (
        dup_groups.groupBy(F.substring("InvoiceDate", 1, 7).alias("month"))
        .agg(F.sum(F.col("count") - 1).alias("excess_rows")).orderBy("month").collect()
    )
    report["duplicates"]["excess_by_month"] = {r["month"]: r["excess_rows"] for r in by_month}
    section("DUPLICATES (all 8 columns identical)")
    print(report["duplicates"])

    # ---- Cardinality -------------------------------------------------------
    c = df.agg(
        F.countDistinct("Customer ID").alias("customers"),
        F.countDistinct("Invoice").alias("invoices"),
        F.countDistinct("Country").alias("countries"),
        F.countDistinct("StockCode").alias("stock_codes"),
        F.countDistinct("Description").alias("descriptions"),
    ).first()
    report["distinct"] = c.asDict()
    section("DISTINCT COUNTS")
    print(report["distinct"])

    # ---- Numeric ranges ----------------------------------------------------
    def num_stats(col):
        r = df.agg(
            F.min(col).alias("min"), F.max(col).alias("max"),
            F.round(F.avg(col), 4).alias("mean"),
            F.percentile_approx(col, [0.01, 0.25, 0.5, 0.75, 0.99], 10000).alias("p01_p25_p50_p75_p99"),
        ).first().asDict()
        return r

    report["quantity"] = num_stats("quantity_t")
    report["price"] = num_stats("price_t")
    dr = df.agg(F.min("date_t").alias("min"), F.max("date_t").alias("max")).first()
    report["invoice_date"] = {"min": str(dr["min"]), "max": str(dr["max"])}
    monthly = (
        df.groupBy(F.date_format("date_t", "yyyy-MM").alias("month")).count().orderBy("month").collect()
    )
    report["rows_by_month"] = {r["month"]: r["count"] for r in monthly}
    section("NUMERIC RANGES")
    print("Quantity:", report["quantity"])
    print("Price   :", report["price"])
    print("InvoiceDate:", report["invoice_date"], f"({len(monthly)} months)")

    # ---- Data issues (detection only) -------------------------------------
    inv_type = (
        F.when(F.col("Invoice").rlike(r"^\d{6}$"), "normal (6 digits)")
        .when(F.col("Invoice").rlike(r"^C\d{6}$"), "cancellation (C + 6 digits)")
        .when(F.col("Invoice").rlike(r"^A\d{6}$"), "adjustment (A + 6 digits)")
        .otherwise("other")
    )
    report["invoice_types"] = {
        r["type"]: {"rows": r["rows"], "invoices": r["invoices"]}
        for r in df.groupBy(inv_type.alias("type"))
        .agg(F.count("*").alias("rows"), F.countDistinct("Invoice").alias("invoices")).collect()
    }
    is_cancel = F.col("Invoice").startswith("C")
    q, p = F.col("quantity_t"), F.col("price_t")
    cnt = lambda cond: df.filter(cond).count()  # noqa: E731
    report["value_issues"] = {
        "quantity_lt_0": cnt(q < 0),
        "quantity_eq_0": cnt(q == 0),
        "quantity_lt_0_and_cancelled": cnt((q < 0) & is_cancel),
        "quantity_lt_0_not_cancelled": cnt((q < 0) & ~is_cancel),
        "cancelled_with_quantity_ge_0": cnt((q >= 0) & is_cancel),
        "price_lt_0": cnt(p < 0),
        "price_eq_0": cnt(p == 0),
        "price_eq_0_with_customer": cnt((p == 0) & F.col("Customer ID").isNotNull()),
        "quantity_lt_0_not_cancelled_with_customer": cnt((q < 0) & ~is_cancel & F.col("Customer ID").isNotNull()),
        "missing_customer_rows": cnt(F.col("Customer ID").isNull()),
        "customer_id_not_5_digits": cnt(F.col("Customer ID").isNotNull() & ~F.col("Customer ID").rlike(r"^\d{5}$")),
        "invoices_without_customer": df.filter(F.col("Customer ID").isNull()).select("Invoice").distinct().count(),
        "customers_with_multiple_countries": df.filter(F.col("Customer ID").isNotNull())
        .groupBy("Customer ID").agg(F.countDistinct("Country").alias("n")).filter("n > 1").count(),
    }

    special = (
        df.filter(~F.col("StockCode").rlike(r"^\d{5}[A-Za-z]*$"))
        .groupBy("StockCode")
        .agg(F.count("*").alias("rows"), F.first("Description", ignorenulls=True).alias("example_description"))
        .orderBy(F.desc("rows"))
    )
    report["non_product_stockcodes"] = {
        "distinct_codes": special.count(),
        "rows": special.agg(F.sum("rows")).first()[0],
        "top": rows_to_dicts(special.limit(15).collect()),
    }
    report["top_countries"] = rows_to_dicts(
        df.groupBy("Country").count().orderBy(F.desc("count")).limit(10).collect()
    )
    report["country_unspecified_rows"] = cnt(F.col("Country") == "Unspecified")

    section("DATA ISSUES (detection only)")
    print("Invoice types:", report["invoice_types"])
    for k, v in report["value_issues"].items():
        print(f"  {k:<45} {v:,}")
    print("Non-product StockCodes:", report["non_product_stockcodes"]["distinct_codes"],
          "codes,", report["non_product_stockcodes"]["rows"], "rows")
    show(special.limit(15))
    print("Top countries:")
    show(df.groupBy("Country").count().orderBy(F.desc("count")).limit(10))

    # ---- Samples -----------------------------------------------------------
    cols = [F.col(f"`{c}`") for c in RAW_COLUMNS]
    samples = {
        "first_rows": df.select(cols).limit(5),
        "cancelled": df.filter(is_cancel).select(cols).limit(3),
        "negative_price": df.filter(p < 0).select(cols).limit(5),
        "zero_price": df.filter(p == 0).select(cols).limit(3),
        "largest_abs_quantity": df.orderBy(F.desc(F.abs(q))).select(cols).limit(4),
        "missing_customer": df.filter(F.col("Customer ID").isNull()).select(cols).limit(3),
        "adjustment_invoices": df.filter(F.col("Invoice").startswith("A")).select(cols).limit(10),
        "cancelled_with_quantity_ge_0": df.filter(is_cancel & (q >= 0)).select(cols).limit(5),
        "negative_quantity_not_cancelled": df.filter((q < 0) & ~is_cancel).select(cols).limit(5),
    }
    report["samples"] = {}
    section("SAMPLE RECORDS")
    for name, sdf in samples.items():
        print(f"-- {name}")
        show(sdf)
        report["samples"][name] = rows_to_dicts(sdf.collect())

    dup_groups.unpersist()
    return report


def sheet_overlap(df: DataFrame, meta: dict) -> dict:
    """Kiểm tra 2 sheet Excel có chứa cùng giao dịch trong khoảng ngày chồng lấn không.

    CSV được ghi theo thứ tự sheet 1 rồi sheet 2, nên N dòng đầu (N = số dòng
    sheet 1 trong metadata) là sheet 1. monotonically_increasing_id giữ thứ tự
    file khi đọc 1 file CSV; row_number đổi nó thành chỉ số dòng liên tục.
    """
    from pyspark.sql import Window

    s1, s2 = meta["sheets"][0], meta["sheets"][1]
    cols = [F.col(f"`{c}`") for c in RAW_COLUMNS]
    indexed = (
        df.select(cols)
        .withColumn("_id", F.monotonically_increasing_id())
        .withColumn("_row", F.row_number().over(Window.orderBy("_id")) - 1)
        .drop("_id")
        .cache()
    )
    sheet1 = indexed.filter(F.col("_row") < s1["rows"]).drop("_row")
    sheet2 = indexed.filter(F.col("_row") >= s1["rows"]).drop("_row")

    start, end = s2["invoice_date_min"], s1["invoice_date_max"]
    # InvoiceDate raw có định dạng cố định yyyy-MM-dd HH:mm:ss -> so sánh chuỗi = so sánh thời gian
    in_window = (F.col("InvoiceDate") >= start) & (F.col("InvoiceDate") <= end)
    w1, w2 = sheet1.filter(in_window), sheet2.filter(in_window)
    result = {
        "overlap_window": [start, end],
        "sheet1_rows_check": sheet1.count(),
        "sheet2_rows_check": sheet2.count(),
        "sheet1_rows_in_window": w1.count(),
        "sheet2_rows_in_window": w2.count(),
        "rows_in_both_sheets": w1.intersectAll(w2).count(),
        "sheet1_window_rows_missing_in_sheet2": w1.exceptAll(w2).count(),
        "sheet2_window_rows_missing_in_sheet1": w2.exceptAll(w1).count(),
        "within_sheet1_duplicate_excess": sheet1.count() - sheet1.distinct().count(),
        "within_sheet2_duplicate_excess": sheet2.count() - sheet2.distinct().count(),
        "combined_duplicate_excess_after_dropping_sheet1_overlap":
            None,
    }
    no_overlap = sheet1.filter(~in_window).unionByName(sheet2)
    result["combined_duplicate_excess_after_dropping_sheet1_overlap"] = (
        no_overlap.count() - no_overlap.distinct().count()
    )
    indexed.unpersist()
    section("SHEET OVERLAP CHECK")
    for k, v in result.items():
        print(f"  {k:<55} {v}")
    return result


def main() -> int:
    cfg = load_config()
    default_path = PROJECT_ROOT / cfg["paths"]["raw"] / cfg["dataset"]["raw_filename"]
    parser = argparse.ArgumentParser(description="Profile raw Online Retail II with PySpark")
    parser.add_argument("--path", default=default_path.as_uri(), help="CSV path (file:// hoặc hdfs://)")
    parser.add_argument("--metadata", default=str(PROJECT_ROOT / cfg["paths"]["raw"] / cfg["dataset"]["metadata_filename"]),
                        help="metadata từ download_dataset.py (bỏ qua overlap check nếu không có)")
    parser.add_argument("--out", default=str(PROJECT_ROOT / cfg["paths"]["output_reports"] / "dataset_profile.json"))
    args = parser.parse_args()

    spark = get_spark("profile-dataset")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark {spark.version} | master={spark.sparkContext.master} | path={args.path}")

    start = time.time()
    df = add_typed_columns(read_raw_csv(spark, args.path, with_corrupt_record=True)).cache()
    report = profile(df, show=lambda d: d.show(truncate=False))
    meta_path = Path(args.metadata)
    if meta_path.exists():
        report["sheet_overlap"] = sheet_overlap(df, json.loads(meta_path.read_text(encoding="utf-8")))
    report["meta"] = {
        "path": args.path,
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "elapsed_seconds": round(time.time() - start, 1),
    }
    df.unpersist()
    spark.stop()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"\nProfile saved to {out} ({report['meta']['elapsed_seconds']}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
