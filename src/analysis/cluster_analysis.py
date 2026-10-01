"""Cluster analysis (Phase 8): hồ sơ (profile) từng cụm dựa trên kết quả phân cụm thực tế.

Input : HDFS output/clustering (CustomerID, Recency, Frequency, Monetary, Cluster) — Phase 6+7
        HDFS processed (để tính Tenure = số ngày từ lần mua ĐẦU TIÊN tới AnalysisDate; chỉ để mô tả, không dùng phân cụm)
Output: HDFS output/analysis/cluster_profile (Parquet)
        output/reports/cluster_profile.{csv,json}      (bảng nhỏ, để vẽ biểu đồ và viết docs)
        output/clustering/customer_clusters.csv         (5,8xx khách, để vẽ phân phối R/F/M theo cụm)

Script chỉ tính SỐ LIỆU. Tên/diễn giải nghiệp vụ của cụm được viết trong docs/08_business_analysis.md
SAU KHI xem profile — không gán nhãn tự động trong code.

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 src/analysis/cluster_analysis.py
"""

import csv
import json
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.settings import PROJECT_ROOT, hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402

METRICS = ["Recency", "Frequency", "Monetary", "Tenure"]


def add_tenure(clustered_df: DataFrame, transactions: DataFrame, analysis_date: str) -> DataFrame:
    """Thêm Tenure = datediff(AnalysisDate, ngày mua đầu tiên) — khách đã gắn bó bao lâu."""
    first = transactions.groupBy("CustomerID").agg(F.min("InvoiceDate").alias("_first"))
    return (
        clustered_df.join(first, "CustomerID", "left")
        .withColumn("Tenure", F.datediff(F.lit(analysis_date).cast("date"), F.to_date("_first")))
        .drop("_first")
    )


def _metric_aggs(prefix: str = "") -> list:
    aggs = [F.count("*").alias("CustomerCount")]
    for m in METRICS:
        aggs += [
            F.round(F.avg(m), 2).alias(f"{prefix}Avg{m}"),
            F.percentile_approx(m, 0.5, 1_000_000).alias(f"{prefix}Median{m}"),
        ]
    aggs += [
        F.round(F.sum("Monetary"), 2).alias("TotalMonetary"),
        F.round(100 * F.avg((F.col("Frequency") == 1).cast("int")), 2).alias("OneTimeBuyerPct"),
    ]
    return aggs


def profile_clusters(clustered_df: DataFrame) -> tuple[DataFrame, dict]:
    """Tạo bảng hồ sơ cho từng cụm (R/F/M/Tenure GỐC) + dòng tổng thể để so sánh."""
    overall = clustered_df.agg(*_metric_aggs()).first().asDict()
    n, total = overall["CustomerCount"], overall["TotalMonetary"]
    profile = (
        clustered_df.groupBy("Cluster").agg(*_metric_aggs())
        .withColumn("CustomerPct", F.round(100 * F.col("CustomerCount") / F.lit(n), 2))
        .withColumn("MonetaryPct", F.round(100 * F.col("TotalMonetary") / F.lit(total), 2))
        # Tỉ lệ so với trung vị toàn bộ khách: > 1 = cao hơn khách "điển hình"
        .withColumn("RecencyVsOverall", F.round(F.col("MedianRecency") / F.lit(overall["MedianRecency"]), 2))
        .withColumn("FrequencyVsOverall", F.round(F.col("MedianFrequency") / F.lit(overall["MedianFrequency"]), 2))
        .withColumn("MonetaryVsOverall", F.round(F.col("MedianMonetary") / F.lit(overall["MedianMonetary"]), 2))
        .orderBy("Cluster")
    )
    return profile, overall


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    from src.utils.spark_session import get_spark

    cfg = load_config()
    ac = cfg["analysis"]
    reports = PROJECT_ROOT / cfg["paths"]["output_reports"]
    analysis_date = json.loads((reports / cfg["rfm"]["report_filename"]).read_text(encoding="utf-8"))["analysis_date"]
    clustering_uri = hdfs_uri(f"{cfg['hdfs']['output_dir']}/clustering")
    out_uri = hdfs_uri(f"{cfg['hdfs']['output_dir']}/analysis/cluster_profile")

    spark = get_spark("phase8-cluster-analysis")
    spark.sparkContext.setLogLevel("WARN")

    print(f"Input : {clustering_uri}  (AnalysisDate {analysis_date})")
    clustered = hdfs_loader.read_parquet(spark, clustering_uri)
    tx = hdfs_loader.read_parquet(spark, hdfs_uri(cfg["hdfs"]["processed_dir"]))
    customers = add_tenure(clustered, tx, analysis_date).cache()
    n = customers.count()

    profile, overall = profile_clusters(customers)
    profile = profile.cache()
    profile.select(
        "Cluster", "CustomerCount", "CustomerPct", "AvgRecency", "AvgFrequency", "AvgMonetary",
        "MedianRecency", "MedianFrequency", "MedianMonetary", "MonetaryPct", "MedianTenure", "OneTimeBuyerPct",
    ).show(truncate=False)
    print("Overall:", overall)

    print(f"Output: {out_uri} (Parquet, overwrite)")
    profile.coalesce(1).write.mode("overwrite").parquet(out_uri)

    rows = [r.asDict() for r in profile.collect()]
    cust_rows = [r.asDict() for r in customers.orderBy("CustomerID").collect()]

    # Kiểm tra tính nhất quán của profile (không có thông tin bị mất khi aggregate)
    checks = {
        "customer_count_sum": sum(r["CustomerCount"] for r in rows) == n == overall["CustomerCount"],
        "monetary_sum": abs(sum(r["TotalMonetary"] for r in rows) - overall["TotalMonetary"]) < 0.05,
        "customer_pct_sum_100": abs(sum(r["CustomerPct"] for r in rows) - 100) < 0.05,
        "monetary_pct_sum_100": abs(sum(r["MonetaryPct"] for r in rows) - 100) < 0.05,
        "no_null_tenure": customers.filter(F.col("Tenure").isNull()).count() == 0,
        "tenure_ge_recency": customers.filter(F.col("Tenure") < F.col("Recency")).count() == 0,
        "clusters_match_k": [r["Cluster"] for r in rows] == list(range(cfg["clustering"]["selected_k"])),
    }
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    passed = all(checks.values())

    customers.unpersist()
    profile.unpersist()
    spark.stop()

    _write_csv(reports / ac["profile_csv"], rows)
    _write_csv(PROJECT_ROOT / cfg["paths"]["output_clustering"] / ac["customers_csv"], cust_rows)
    (reports / ac["profile_json"]).write_text(json.dumps({
        "input": clustering_uri, "analysis_date": analysis_date, "customers": n,
        "k": cfg["clustering"]["selected_k"], "overall": overall, "clusters": rows,
        "hdfs_output": out_uri, "checks": checks, "result": "PASS" if passed else "FAIL",
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"export -> {reports / ac['profile_csv']}, {reports / ac['profile_json']}, "
          f"{PROJECT_ROOT / cfg['paths']['output_clustering'] / ac['customers_csv']}")
    print("\nRESULT:", "PASS" if passed else "FAIL")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
