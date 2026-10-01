"""Kiểm tra Phase 4: đọc lại Parquet processed từ HDFS bằng Spark và xác nhận dữ liệu sạch.

Chạy BÊN TRONG container spark-master, sau src/preprocessing/clean_transactions.py:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 scripts/check_processed_data.py

Kiểm tra: schema · row count (khớp báo cáo preprocessing) · missing · duplicates ·
invalid values (Quantity/UnitPrice/Amount, hóa đơn hủy/điều chỉnh, mã không phải sản phẩm) ·
Amount = Quantity * UnitPrice · số dòng trước/sau từng rule cộng lại khớp.
Số kỳ vọng lấy từ output/reports/{hdfs_ingestion_check,preprocessing_report}.json (không hard-code).
Kết quả ghi ra output/reports/processed_check.json. Exit code 0 = PASS, 1 = FAIL.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from src.config.settings import PROJECT_ROOT, hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402
from src.preprocessing.clean_transactions import OUTPUT_COLUMNS  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402

EXPECTED_TYPES = {
    "InvoiceNo": "string", "StockCode": "string", "Description": "string", "Quantity": "int",
    "InvoiceDate": "timestamp_ntz", "UnitPrice": "double", "CustomerID": "string",
    "Country": "string", "Amount": "double",
}


def step(n: int, title: str) -> None:
    print(f"\n[{n}] {title}\n" + "-" * 70)


def main() -> int:
    cfg = load_config()
    pp = cfg["preprocessing"]
    reports = PROJECT_ROOT / cfg["paths"]["output_reports"]
    prep = json.loads((reports / pp["report_filename"]).read_text(encoding="utf-8"))
    ingestion = json.loads((reports / "hdfs_ingestion_check.json").read_text(encoding="utf-8"))
    uri = hdfs_uri(cfg["hdfs"]["processed_dir"])
    checks: dict[str, bool] = {}

    spark = get_spark("phase4-processed-check")
    spark.sparkContext.setLogLevel("WARN")

    step(1, f"Parquet on HDFS: {uri}")
    files = [f for f in hdfs_loader.list_dir(spark, uri) if f["path"].endswith(".parquet")]
    for f in files:
        print(f"  {f['path'].rsplit('/', 1)[-1]}  {f['bytes']:,} bytes  replication={f['replication']}")
    checks["parquet_files_exist"] = len(files) > 0
    checks["parquet_file_count_as_config"] = len(files) == pp["output_files"]

    step(2, "Read back with Spark -> schema")
    df = hdfs_loader.read_parquet(spark, uri).cache()
    df.printSchema()
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    checks["schema_matches"] = list(types) == OUTPUT_COLUMNS and types == EXPECTED_TYPES

    step(3, "Row count")
    rows = df.count()
    removed = sum(r["records_removed"] for r in prep["rules"])
    print(f"rows={rows:,}  partitions={df.rdd.getNumPartitions()}  report output_rows={prep['output_rows']:,}")
    print(f"raw {ingestion['records']:,} - removed {removed:,} = {ingestion['records'] - removed:,}")
    checks["row_count_matches_report"] = rows == prep["output_rows"]
    checks["rule_accounting_matches_raw"] = (
        prep["input_rows"] == ingestion["records"] and ingestion["records"] - removed == rows
    )

    step(4, "Missing values")
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in OUTPUT_COLUMNS]).first().asDict()
    print(nulls)
    checks["no_nulls"] = all(v == 0 for v in nulls.values())
    blank_customer = df.filter(F.trim("CustomerID") == "").count()
    checks["no_blank_customer_id"] = blank_customer == 0

    step(5, "Duplicates (8 transaction columns)")
    dup = rows - df.dropDuplicates([c for c in OUTPUT_COLUMNS if c != "Amount"]).count()
    print(f"duplicate rows={dup}")
    checks["no_duplicates"] = dup == 0

    step(6, "Invalid values")
    invalid = {
        "quantity_le_0": df.filter(F.col("Quantity") <= 0).count(),
        "unit_price_le_0": df.filter(F.col("UnitPrice") <= 0).count(),
        "amount_le_0": df.filter(F.col("Amount") <= 0).count(),
        "invoice_not_valid_pattern": df.filter(~F.col("InvoiceNo").rlike(pp["valid_invoice_pattern"])).count(),
        "cancelled_invoices": df.filter(F.col("InvoiceNo").startswith(pp["cancel_prefix"])).count(),
        "non_product_stockcodes": df.filter(F.col("StockCode").isin(pp["non_product_stockcodes"])).count(),
        "amount_ne_quantity_x_price": df.filter(
            F.abs(F.col("Amount") - F.col("Quantity") * F.col("UnitPrice")) > 1e-6
        ).count(),
    }
    for k, v in invalid.items():
        print(f"  {k:<28} {v}")
    checks["no_invalid_values"] = all(v == 0 for v in invalid.values())

    step(7, "Summary + sample")
    s = df.agg(
        F.countDistinct("CustomerID").alias("customers"),
        F.countDistinct("InvoiceNo").alias("invoices"),
        F.min("InvoiceDate").alias("date_min"),
        F.max("InvoiceDate").alias("date_max"),
        F.round(F.sum("Amount"), 2).alias("amount_total"),
    ).first().asDict()
    print({k: str(v) for k, v in s.items()})
    checks["summary_matches_report"] = (
        s["customers"] == prep["summary"]["customers"] and s["amount_total"] == prep["summary"]["amount_total"]
    )
    df.orderBy("InvoiceDate", "InvoiceNo", "StockCode").show(5, truncate=False)
    df.unpersist()

    step(8, "Checks")
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    passed = all(checks.values())
    out = reports / "processed_check.json"
    out.write_text(json.dumps({
        "hdfs_uri": uri, "parquet_files": len(files), "rows": rows, "schema": types,
        "nulls": nulls, "duplicates": dup, "invalid": invalid,
        "summary": {k: str(v) for k, v in s.items()},
        "checks": checks, "result": "PASS" if passed else "FAIL",
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nreport -> {out}")
    print("\nRESULT:", "PASS" if passed else "FAIL")
    spark.stop()
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
