"""Kiểm tra Phase 6+7: đọc lại kết quả phân cụm từ HDFS và xác nhận khớp RFM + báo cáo.

Chạy BÊN TRONG container spark-master, sau src/clustering/evaluate.py và src/clustering/kmeans.py:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 scripts/check_clustering.py

Kiểm tra: schema · số khách = RFM · CustomerID duy nhất · không NULL · Cluster trong [0, K-1], đủ K cụm ·
R/F/M trong output trùng bảng RFM gốc · cluster size khớp kmeans_report · Silhouette tính lại từ output
khớp báo cáo và khớp dòng K tương ứng trong k_evaluation (tái lập với cùng seed) · K-Means hội tụ.
Exit code 0 = PASS, 1 = FAIL.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from src.clustering.evaluate import silhouette_score  # noqa: E402
from src.clustering.kmeans import CLUSTER_COL, scale_features  # noqa: E402
from src.config.settings import PROJECT_ROOT, hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402

EXPECTED_TYPES = {"CustomerID": "string", "Recency": "int", "Frequency": "bigint", "Monetary": "double", "Cluster": "int"}


def step(n: int, title: str) -> None:
    print(f"\n[{n}] {title}\n" + "-" * 70)


def main() -> int:
    cfg = load_config()
    cc = cfg["clustering"]
    k = cc["selected_k"]
    features = cc["features"]
    rep = json.loads((PROJECT_ROOT / cfg["paths"]["output_reports"] / cc["kmeans_report"]).read_text(encoding="utf-8"))
    ev = json.loads((PROJECT_ROOT / cfg["paths"]["output_evaluation"] / cc["evaluation_report"]).read_text(encoding="utf-8"))
    variant = "log1p_standard_scaler" if cc["log_transform"] else "standard_scaler"
    ev_row = next(r for r in ev["variants"][variant]["results"] if r["k"] == k)
    out_uri = hdfs_uri(f"{cfg['hdfs']['output_dir']}/clustering")
    checks: dict[str, bool] = {}

    spark = get_spark("phase6-clustering-check")
    spark.sparkContext.setLogLevel("WARN")

    step(1, f"Clustering output on HDFS: {out_uri}")
    out = hdfs_loader.read_parquet(spark, out_uri).cache()
    out.printSchema()
    types = {f.name: f.dataType.simpleString() for f in out.schema.fields}
    checks["schema_matches"] = types == EXPECTED_TYPES and list(types) == list(EXPECTED_TYPES)

    step(2, "Customers / null / unique / cluster ids")
    rfm = hdfs_loader.read_parquet(spark, hdfs_uri(cfg["hdfs"]["rfm_dir"])).cache()
    n, n_rfm = out.count(), rfm.count()
    nulls = out.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in EXPECTED_TYPES]).first().asDict()
    dup = n - out.select("CustomerID").distinct().count()
    sizes = {r[CLUSTER_COL]: r["count"] for r in out.groupBy(CLUSTER_COL).count().collect()}
    print(f"rows={n:,}  rfm={n_rfm:,}  nulls={nulls}  duplicate CustomerID={dup}")
    print(f"cluster sizes={dict(sorted(sizes.items()))}  report={rep['cluster_sizes']}")
    checks["customer_count_matches_rfm"] = n == n_rfm == rep["customers"]
    checks["no_nulls"] = all(v == 0 for v in nulls.values())
    checks["customer_id_unique"] = dup == 0
    checks["k_clusters_all_non_empty"] = sorted(sizes) == list(range(k))
    checks["cluster_sizes_match_report"] = [sizes[i] for i in range(k)] == rep["cluster_sizes"]
    checks["cluster_sizes_match_evaluation"] = sorted(sizes.values(), reverse=True) == ev_row["cluster_sizes"]

    step(3, "R/F/M in output == RFM table (giá trị gốc, chưa scale)")
    diff = (
        out.alias("o").join(rfm.alias("r"), "CustomerID", "full_outer")
        .filter(" OR ".join(f"NOT (o.{c} <=> r.{c})" for c in features)).count()
    )
    print(f"rows with different R/F/M = {diff}")
    checks["rfm_values_unchanged"] = diff == 0

    step(4, "Recompute Silhouette from output clusters")
    scaled, _ = scale_features(rfm, features, cc["log_transform"])
    pred = scaled.join(out.select("CustomerID", CLUSTER_COL), "CustomerID")
    sil = round(silhouette_score(pred), 4)
    print(f"silhouette recomputed={sil}  kmeans_report={rep['silhouette']}  k_evaluation(k={k})={ev_row['silhouette']}")
    checks["silhouette_matches_report"] = sil == rep["silhouette"] == ev_row["silhouette"]
    checks["kmeans_converged"] = rep["num_iter"] < rep["max_iter"]

    step(5, "Checks")
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    passed = all(checks.values())
    report_out = PROJECT_ROOT / cfg["paths"]["output_reports"] / "clustering_check.json"
    report_out.write_text(json.dumps({
        "hdfs_uri": out_uri, "rows": n, "schema": types, "nulls": nulls, "cluster_sizes": sizes,
        "rfm_value_diffs": diff, "silhouette_recomputed": sil, "checks": checks,
        "result": "PASS" if passed else "FAIL",
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nreport -> {report_out}")
    print("\nRESULT:", "PASS" if passed else "FAIL")
    spark.stop()
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
