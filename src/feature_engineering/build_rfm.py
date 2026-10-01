"""Feature engineering (Phase 5): tính RFM cho từng khách hàng bằng Spark aggregation.

Transaction-level (1 dòng = 1 sản phẩm trong 1 hóa đơn)  ->  customer-level (1 dòng = 1 khách):

- Recency   : số ngày từ ngày mua gần nhất của khách đến AnalysisDate
              = datediff(AnalysisDate, to_date(max(InvoiceDate))).
- Frequency : số hóa đơn (InvoiceNo) khác nhau = số lần mua hàng thực tế
              (không đếm số dòng: 1 hóa đơn 20 sản phẩm vẫn là 1 lần mua).
- Monetary  : tổng Amount (= Quantity * UnitPrice, tạo ở Phase 4).

AnalysisDate (config rfm.analysis_date): null -> to_date(max(InvoiceDate) của processed) + offset ngày.

Input : Parquet processed (hdfs.processed_dir).
Output: Parquet RFM (hdfs.rfm_dir) — CustomerID, Recency, Frequency, Monetary
        + output/reports/rfm_report.json (AnalysisDate, thống kê, outlier, mẫu).

Chạy BÊN TRONG container spark-master:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 src/feature_engineering/build_rfm.py
"""

import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.settings import PROJECT_ROOT, hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402

FEATURES = ["Recency", "Frequency", "Monetary"]
PERCENTILES = [0.25, 0.5, 0.75, 0.9, 0.99]


def resolve_analysis_date(df: DataFrame, rfm_cfg: dict) -> dict:
    """Xác định AnalysisDate theo config; trả về dict gồm ngày và rule đã dùng."""
    max_ts = df.agg(F.max("InvoiceDate")).first()[0]
    if rfm_cfg.get("analysis_date"):
        d = date.fromisoformat(str(rfm_cfg["analysis_date"]))
        rule = "cố định trong config (rfm.analysis_date)"
    else:
        offset = int(rfm_cfg.get("analysis_date_offset_days", 1))
        d = max_ts.date() + timedelta(days=offset)
        rule = f"to_date(max(InvoiceDate) của processed) + {offset} ngày"
    return {"analysis_date": d, "rule": rule, "max_invoice_date": str(max_ts)}


def build_rfm(df: DataFrame, analysis_date: date, customer_col: str = "CustomerID") -> DataFrame:
    """Nhận DataFrame giao dịch đã làm sạch, trả về DataFrame RFM (1 dòng / khách)."""
    return (
        df.groupBy(customer_col)
        .agg(
            F.max("InvoiceDate").alias("_last_purchase"),
            F.countDistinct("InvoiceNo").alias("Frequency"),
            F.round(F.sum("Amount"), 2).alias("Monetary"),
        )
        # So theo NGÀY: giờ trong ngày không có ý nghĩa với hành vi mua hàng
        .withColumn("Recency", F.datediff(F.lit(analysis_date), F.to_date("_last_purchase")))
        .select(customer_col, *FEATURES)
    )


def describe_rfm(rfm: DataFrame, customer_col: str = "CustomerID") -> dict:
    """Thống kê mô tả + outlier/skew cho R, F, M (bảng nhỏ, vẫn tính bằng Spark)."""
    aggs = []
    for c in FEATURES:
        aggs += [
            F.min(c).alias(f"{c}|min"), F.max(c).alias(f"{c}|max"),
            F.avg(c).alias(f"{c}|mean"), F.stddev(c).alias(f"{c}|std"),
            F.skewness(c).alias(f"{c}|skewness"),
            # accuracy rất lớn -> percentile chính xác với vài nghìn dòng
            F.percentile_approx(c, PERCENTILES, 1_000_000).alias(f"{c}|pct"),
        ]
    r = rfm.agg(*aggs).first().asDict()

    stats, outliers = {}, {}
    for c in FEATURES:
        p25, p50, p75, p90, p99 = r[f"{c}|pct"]
        stats[c] = {
            "min": r[f"{c}|min"], "p25": p25, "median": p50, "p75": p75, "p90": p90, "p99": p99,
            "max": r[f"{c}|max"],
            "mean": round(r[f"{c}|mean"], 2), "std": round(r[f"{c}|std"], 2),
            "skewness": round(r[f"{c}|skewness"], 2),
        }
        # Quy tắc IQR (Tukey): ngoài [Q1 - 1.5 IQR, Q3 + 1.5 IQR] là outlier. Chỉ ĐẾM, không xóa.
        iqr = p75 - p25
        hi, lo = p75 + 1.5 * iqr, p25 - 1.5 * iqr
        outliers[c] = {
            "iqr_upper_fence": round(hi, 2),
            "above_upper_fence": rfm.filter(F.col(c) > hi).count(),
            "below_lower_fence": rfm.filter(F.col(c) < lo).count(),
        }

    def top(col: str, n: int = 5) -> list[dict]:
        return [row.asDict() for row in rfm.orderBy(F.desc(col), customer_col).limit(n).collect()]

    total_monetary = rfm.agg(F.sum("Monetary")).first()[0]
    top1pct = int(round(rfm.count() * 0.01))
    top_share = rfm.orderBy(F.desc("Monetary")).limit(top1pct).agg(F.sum("Monetary")).first()[0]
    return {
        "stats": stats,
        "outliers_iqr": outliers,
        "correlation": {
            "Recency_Frequency": round(rfm.stat.corr("Recency", "Frequency"), 3),
            "Recency_Monetary": round(rfm.stat.corr("Recency", "Monetary"), 3),
            "Frequency_Monetary": round(rfm.stat.corr("Frequency", "Monetary"), 3),
        },
        "frequency_eq_1": rfm.filter(F.col("Frequency") == 1).count(),
        "monetary_total": round(total_monetary, 2),
        "top_1pct_customers": top1pct,
        "top_1pct_monetary_share_pct": round(100 * top_share / total_monetary, 2),
        "top_monetary": top("Monetary"),
        "top_frequency": top("Frequency"),
        "top_recency": top("Recency"),
        "sample_customers": [row.asDict() for row in rfm.orderBy(customer_col).limit(5).collect()],
    }


def main() -> int:
    from src.utils.spark_session import get_spark

    cfg = load_config()
    rfm_cfg = cfg["rfm"]
    customer_col = rfm_cfg["customer_col"]
    in_uri = hdfs_uri(cfg["hdfs"]["processed_dir"])
    out_uri = hdfs_uri(cfg["hdfs"]["rfm_dir"])
    report_path = PROJECT_ROOT / cfg["paths"]["output_reports"] / rfm_cfg["report_filename"]

    spark = get_spark("phase5-rfm")
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.perf_counter()

    print(f"Input : {in_uri}")
    tx = hdfs_loader.read_parquet(spark, in_uri).cache()
    tx_rows = tx.count()

    ad = resolve_analysis_date(tx, rfm_cfg)
    print(f"AnalysisDate = {ad['analysis_date']}  ({ad['rule']}; max InvoiceDate = {ad['max_invoice_date']})")

    rfm = build_rfm(tx, ad["analysis_date"], customer_col).cache()
    customers = rfm.count()
    print(f"transactions={tx_rows:,} -> customers={customers:,}")
    rfm.printSchema()

    desc = describe_rfm(rfm, customer_col)
    print(f"\n{'':<10}{'min':>10}{'p25':>10}{'median':>10}{'p75':>10}{'p99':>12}{'max':>12}{'mean':>12}{'std':>12}{'skew':>8}")
    for c, s in desc["stats"].items():
        print(f"{c:<10}{s['min']:>10}{s['p25']:>10}{s['median']:>10}{s['p75']:>10}{s['p99']:>12}"
              f"{s['max']:>12}{s['mean']:>12}{s['std']:>12}{s['skewness']:>8}")
    print("\nIQR outliers (chỉ đếm, không xóa):", {c: o["above_upper_fence"] for c, o in desc["outliers_iqr"].items()})
    print("Correlation:", desc["correlation"])
    print(f"Frequency = 1: {desc['frequency_eq_1']:,} khách; top 1% khách ({desc['top_1pct_customers']}) "
          f"chiếm {desc['top_1pct_monetary_share_pct']}% Monetary")
    print("\nSample customers:")
    rfm.orderBy(customer_col).show(5)
    print("Top Monetary:")
    rfm.orderBy(F.desc("Monetary")).show(5)

    print(f"Output: {out_uri} (Parquet, overwrite)")
    rfm.coalesce(rfm_cfg["output_files"]).write.mode("overwrite").parquet(out_uri)
    files = [f for f in hdfs_loader.list_dir(spark, out_uri) if f["path"].endswith(".parquet")]

    report = {
        "input": {"hdfs_uri": in_uri, "transactions": tx_rows},
        "analysis_date": str(ad["analysis_date"]),
        "analysis_date_rule": ad["rule"],
        "max_invoice_date": ad["max_invoice_date"],
        "definitions": {
            "Recency": "datediff(AnalysisDate, to_date(max(InvoiceDate))) — số ngày",
            "Frequency": "countDistinct(InvoiceNo) — số hóa đơn khác nhau",
            "Monetary": "round(sum(Amount), 2) — tổng tiền mua hàng",
        },
        "customers": customers,
        **desc,
        "output": {
            "hdfs_uri": out_uri,
            "parquet_files": len(files),
            "bytes": sum(f["bytes"] for f in files),
            "schema": [[f.name, f.dataType.simpleString()] for f in rfm.schema.fields],
        },
        "elapsed_seconds": round(time.perf_counter() - t0, 1),
        "spark_version": spark.version,
    }
    rfm.unpersist()
    tx.unpersist()
    spark.stop()

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"parquet files={len(files)}, bytes={report['output']['bytes']:,}, elapsed={report['elapsed_seconds']}s")
    print(f"report -> {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
