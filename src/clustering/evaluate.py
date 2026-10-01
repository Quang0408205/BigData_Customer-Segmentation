"""Evaluation (Phase 7): đánh giá K-Means cho K = k_min..k_max để chọn số cụm.

Metric cho mỗi K:
- Silhouette Score (ClusteringEvaluator, squared Euclidean): cụm tách bạch hay chồng lấn, [-1, 1], càng cao càng tốt.
- Cluster size: số khách mỗi cụm — loại K tạo ra cụm quá nhỏ (vài khách) khó dùng và không ổn định.
- Processing time: thời gian fit + transform + evaluate.
- Metric bổ sung (giải thích trong docs/07_evaluation.md):
  - WSSSE (model.summary.trainingCost): tổng bình phương khoảng cách điểm -> centroid. Luôn giảm khi K tăng,
    dùng để xem "khuỷu tay" (elbow), không dùng một mình để chọn K.
  - numIter: số vòng lặp thật, để biết K-Means đã hội tụ (numIter < maxIter).
  - Trung bình R/F/M gốc theo cụm: để đánh giá khả năng diễn giải (KHÔNG đặt tên cụm ở đây).

So sánh 2 cách scaling: StandardScaler trên giá trị gốc và log1p + StandardScaler.

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 src/clustering/evaluate.py

Output: output/evaluation/k_evaluation.{json,csv} (local, để vẽ biểu đồ) + HDFS evaluation/k_selection (Parquet).
"""

import csv
import json
import sys
import time
from pathlib import Path

from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.clustering.kmeans import (  # noqa: E402
    CLUSTER_COL, FEATURES_COL, cluster_rfm_means, scale_features, scaling_summary, train_kmeans,
)
from src.config.settings import PROJECT_ROOT, hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402

VARIANTS = {"standard_scaler": False, "log1p_standard_scaler": True}


def silhouette_score(predictions_df: DataFrame) -> float:
    """Silhouette Score của kết quả phân cụm (MLlib mặc định dùng squared Euclidean distance)."""
    return ClusteringEvaluator(
        featuresCol=FEATURES_COL, predictionCol=CLUSTER_COL,
        metricName="silhouette", distanceMeasure="squaredEuclidean",
    ).evaluate(predictions_df)


def evaluate_k_range(
    scaled_df: DataFrame, k_min: int, k_max: int, seed: int = 42,
    max_iter: int = 100, tol: float = 1e-4, feature_cols: list[str] | None = None,
) -> list[dict]:
    """Chạy K-Means cho từng k trong [k_min, k_max], trả về bảng k -> metric."""
    rows = []
    for k in range(k_min, k_max + 1):
        t0 = time.perf_counter()
        model = train_kmeans(scaled_df, k, seed, max_iter, tol)
        pred = model.transform(scaled_df).cache()
        sil = silhouette_score(pred)
        seconds = round(time.perf_counter() - t0, 2)
        sizes = sorted(model.summary.clusterSizes, reverse=True)
        n = sum(sizes)
        rows.append({
            "k": k,
            "silhouette": round(sil, 4),
            "cluster_sizes": sizes,
            "min_cluster_size": sizes[-1],
            "min_cluster_pct": round(100 * sizes[-1] / n, 2),
            "wssse": round(model.summary.trainingCost, 2),
            "num_iter": model.summary.numIter,
            "seconds": seconds,
            "cluster_rfm_means": cluster_rfm_means(pred, feature_cols) if feature_cols else None,
        })
        pred.unpersist()
        r = rows[-1]
        print(f"  k={k}  silhouette={r['silhouette']:.4f}  sizes={sizes}  min={r['min_cluster_pct']}%  "
              f"WSSSE={r['wssse']:.1f}  iter={r['num_iter']}  {seconds}s")
    return rows


def main() -> int:
    from src.utils.spark_session import get_spark

    cfg = load_config()
    cc = cfg["clustering"]
    features = cc["features"]
    rfm_uri = hdfs_uri(cfg["hdfs"]["rfm_dir"])
    out_dir = PROJECT_ROOT / cfg["paths"]["output_evaluation"]
    hdfs_out = hdfs_uri(f"{cfg['hdfs']['evaluation_dir']}/k_selection")

    spark = get_spark("phase7-evaluate-k")
    spark.sparkContext.setLogLevel("WARN")
    t_all = time.perf_counter()
    rfm = hdfs_loader.read_parquet(spark, rfm_uri).cache()
    n = rfm.count()
    print(f"Input: {rfm_uri}  customers={n:,}  K={cc['k_min']}..{cc['k_max']}  seed={cc['seed']}")

    report = {"input": rfm_uri, "customers": n, "features": features, "seed": cc["seed"],
              "max_iter": cc["max_iter"], "tol": cc["tol"], "variants": {}}
    for name, log in VARIANTS.items():
        print(f"\n=== {name} (log1p={log}) ===")
        scaled, _ = scale_features(rfm, features, log)
        scaled = scaled.cache()
        scaled.count()
        scaling = scaling_summary(scaled, features, log)
        for c, s in scaling.items():
            print(f"  {c:<10} {s}")
        rows = evaluate_k_range(scaled, cc["k_min"], cc["k_max"], cc["seed"], cc["max_iter"], cc["tol"], features)
        report["variants"][name] = {"log_transform": log, "scaling_summary": scaling, "results": rows}
        scaled.unpersist()

    report["elapsed_seconds"] = round(time.perf_counter() - t_all, 1)

    # Bảng phẳng: CSV local (vẽ biểu đồ ở Phase 8+10) + Parquet trên HDFS
    flat = [
        {"variant": v, "k": r["k"], "silhouette": r["silhouette"], "min_cluster_size": r["min_cluster_size"],
         "min_cluster_pct": r["min_cluster_pct"], "cluster_sizes": " ".join(map(str, r["cluster_sizes"])),
         "wssse": r["wssse"], "num_iter": r["num_iter"], "seconds": r["seconds"]}
        for v, d in report["variants"].items() for r in d["results"]
    ]
    spark.createDataFrame(flat).coalesce(1).write.mode("overwrite").parquet(hdfs_out)
    report["hdfs_output"] = hdfs_out
    rfm.unpersist()
    spark.stop()

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / cc["evaluation_report"]).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    with open(out_dir / Path(cc["evaluation_report"]).with_suffix(".csv").name, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(flat[0]))
        w.writeheader()
        w.writerows(flat)
    print(f"\nelapsed={report['elapsed_seconds']}s  report -> {out_dir / cc['evaluation_report']} (+ .csv), {hdfs_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
