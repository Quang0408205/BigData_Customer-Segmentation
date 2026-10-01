"""Kiểm tra Phase 5: đọc lại RFM Parquet từ HDFS và xác nhận đúng với dữ liệu processed.

Chạy BÊN TRONG container spark-master, sau src/feature_engineering/build_rfm.py:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 scripts/check_rfm.py

Kiểm tra: schema · customer count = số CustomerID distinct trong processed · null · CustomerID trùng ·
miền giá trị (Recency >= offset, Frequency >= 1, Monetary > 0) · tính lại RFM bằng Spark SQL
(cách viết độc lập với build_rfm.py) và so từng khách · tổng Monetary = tổng Amount ·
tổng Frequency = số cặp (CustomerID, InvoiceNo).
Số kỳ vọng lấy từ output/reports/{preprocessing_report,rfm_report}.json. Exit code 0 = PASS, 1 = FAIL.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from src.config.settings import PROJECT_ROOT, hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402

EXPECTED_TYPES = {"CustomerID": "string", "Recency": "int", "Frequency": "bigint", "Monetary": "double"}


def step(n: int, title: str) -> None:
    print(f"\n[{n}] {title}\n" + "-" * 70)


def main() -> int:
    cfg = load_config()
    rfm_cfg = cfg["rfm"]
    reports = PROJECT_ROOT / cfg["paths"]["output_reports"]
    prep = json.loads((reports / cfg["preprocessing"]["report_filename"]).read_text(encoding="utf-8"))
    rep = json.loads((reports / rfm_cfg["report_filename"]).read_text(encoding="utf-8"))
    rfm_uri, tx_uri = hdfs_uri(cfg["hdfs"]["rfm_dir"]), hdfs_uri(cfg["hdfs"]["processed_dir"])
    analysis_date = rep["analysis_date"]
    checks: dict[str, bool] = {}

    spark = get_spark("phase5-rfm-check")
    spark.sparkContext.setLogLevel("WARN")

    step(1, f"RFM Parquet on HDFS: {rfm_uri}")
    files = [f for f in hdfs_loader.list_dir(spark, rfm_uri) if f["path"].endswith(".parquet")]
    for f in files:
        print(f"  {f['path'].rsplit('/', 1)[-1]}  {f['bytes']:,} bytes")
    checks["parquet_file_count_as_config"] = len(files) == rfm_cfg["output_files"]

    step(2, "Schema")
    rfm = hdfs_loader.read_parquet(spark, rfm_uri).cache()
    rfm.printSchema()
    types = {f.name: f.dataType.simpleString() for f in rfm.schema.fields}
    checks["schema_matches"] = types == EXPECTED_TYPES and list(types) == list(EXPECTED_TYPES)

    step(3, "Customer count / null / duplicate CustomerID")
    tx = hdfs_loader.read_parquet(spark, tx_uri).cache()
    n = rfm.count()
    n_tx_customers = tx.select("CustomerID").distinct().count()
    nulls = rfm.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in EXPECTED_TYPES]).first().asDict()
    dup_ids = n - rfm.select("CustomerID").distinct().count()
    print(f"rfm rows={n:,}  distinct CustomerID in processed={n_tx_customers:,}  "
          f"Phase 4 report={prep['summary']['customers']:,}  rfm report={rep['customers']:,}")
    print(f"nulls={nulls}  duplicate CustomerID={dup_ids}")
    checks["customer_count_matches"] = n == n_tx_customers == prep["summary"]["customers"] == rep["customers"]
    checks["no_nulls"] = all(v == 0 for v in nulls.values())
    checks["customer_id_unique"] = dup_ids == 0

    step(4, "Value ranges")
    rng = rfm.agg(*[F.min(c).alias(f"min_{c}") for c in ["Recency", "Frequency", "Monetary"]],
                  *[F.max(c).alias(f"max_{c}") for c in ["Recency", "Frequency", "Monetary"]]).first().asDict()
    print(rng)
    span = tx.agg(F.datediff(F.lit(analysis_date).cast("date"), F.to_date(F.min("InvoiceDate")))).first()[0]
    checks["recency_in_range"] = rng["min_Recency"] >= rfm_cfg["analysis_date_offset_days"] and rng["max_Recency"] <= span
    checks["frequency_ge_1"] = rng["min_Frequency"] >= 1
    checks["monetary_gt_0"] = rng["min_Monetary"] > 0

    step(5, f"Recompute RFM with Spark SQL (AnalysisDate = {analysis_date}) and compare per customer")
    tx.createOrReplaceTempView("tx")
    expected = spark.sql(f"""
        SELECT CustomerID,
               DATEDIFF(DATE '{analysis_date}', TO_DATE(MAX(InvoiceDate))) AS r,
               COUNT(DISTINCT InvoiceNo)                                AS f,
               ROUND(SUM(Amount), 2)                                    AS m
        FROM tx GROUP BY CustomerID
    """)
    joined = expected.join(rfm, "CustomerID", "full_outer")
    mismatch = joined.filter(
        F.col("Recency").isNull() | F.col("r").isNull()
        | (F.col("Recency") != F.col("r")) | (F.col("Frequency") != F.col("f"))
        | (F.abs(F.col("Monetary") - F.col("m")) > 0.005)
    ).count()
    print(f"customers compared={joined.count():,}  mismatches={mismatch}")
    checks["recomputed_rfm_matches"] = mismatch == 0

    step(6, "Totals")
    tot = rfm.agg(F.sum("Frequency").alias("f"), F.sum("Monetary").alias("m")).first()
    pairs = tx.select("CustomerID", "InvoiceNo").distinct().count()
    amount = tx.agg(F.sum("Amount")).first()[0]
    multi_customer_invoices = tx.groupBy("InvoiceNo").agg(F.countDistinct("CustomerID").alias("n")).filter("n > 1").count()
    print(f"sum Frequency={tot['f']:,}  distinct (CustomerID, InvoiceNo)={pairs:,}  "
          f"invoices with >1 customer={multi_customer_invoices}")
    print(f"sum Monetary={tot['m']:,.2f}  sum Amount (processed)={amount:,.2f}")
    checks["frequency_total_matches"] = tot["f"] == pairs
    # Monetary làm tròn 2 số lẻ theo từng khách -> chênh tối đa 0.005 x số khách
    checks["monetary_total_matches"] = abs(tot["m"] - amount) <= 0.005 * n

    step(7, "Checks")
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    passed = all(checks.values())
    out = reports / "rfm_check.json"
    out.write_text(json.dumps({
        "hdfs_uri": rfm_uri, "rows": n, "schema": types, "nulls": nulls, "duplicate_customer_ids": dup_ids,
        "ranges": rng, "recompute_mismatches": mismatch,
        "sum_frequency": tot["f"], "distinct_customer_invoice_pairs": pairs,
        "invoices_with_multiple_customers": multi_customer_invoices,
        "sum_monetary": round(tot["m"], 2), "sum_amount_processed": round(amount, 2),
        "checks": checks, "result": "PASS" if passed else "FAIL",
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nreport -> {out}")
    print("\nRESULT:", "PASS" if passed else "FAIL")
    rfm.unpersist()
    tx.unpersist()
    spark.stop()
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
