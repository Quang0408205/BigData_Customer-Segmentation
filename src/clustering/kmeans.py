"""Clustering (Phase 6): feature scaling + K-Means bằng Spark MLlib.

    RFM -> [log1p] -> VectorAssembler -> StandardScaler -> K-Means -> Cluster

- log1p (tùy chọn, config clustering.log_transform): giảm lệch phải của Frequency/Monetary
  để vài khách cực lớn không chi phối khoảng cách.
- VectorAssembler: gộp các cột số thành 1 cột vector (MLlib chỉ nhận vector).
- StandardScaler(withMean, withStd): z = (x - mean) / std -> mọi đặc trưng cùng thang đo.
- KMeans: khởi tạo k-means||, lặp gán điểm -> cập nhật centroid tới khi hội tụ (tol) hoặc đạt maxIter.

Chạy model cuối (K = clustering.selected_k, chọn sau khi chạy src/clustering/evaluate.py):

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 src/clustering/kmeans.py

Output: HDFS output/clustering (CustomerID, Recency, Frequency, Monetary, Cluster — R/F/M gốc)
        + output/reports/kmeans_report.json.
"""

import json
import sys
import time
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.settings import PROJECT_ROOT, hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402

FEATURES_COL = "features"
CLUSTER_COL = "Cluster"


def scale_features(
    rfm_df: DataFrame, feature_cols: list[str], log_transform: bool = True
) -> tuple[DataFrame, PipelineModel]:
    """Gộp các cột RFM thành vector và chuẩn hóa; trả về (DataFrame có cột 'features', model scaling)."""
    df, cols = rfm_df, list(feature_cols)
    if log_transform:
        # log1p(x) = ln(1 + x): xác định với x = 0, giữ thứ tự, nén giá trị lớn
        df = df.select("*", *[F.log1p(F.col(c)).alias(f"log_{c}") for c in feature_cols])
        cols = [f"log_{c}" for c in feature_cols]
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=cols, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol=FEATURES_COL, withMean=True, withStd=True),
    ])
    model = pipeline.fit(df)
    return model.transform(df), model


def scaling_summary(scaled_df: DataFrame, feature_cols: list[str], log_transform: bool) -> dict:
    """mean/std của từng đặc trưng: gốc -> (log1p) -> sau StandardScaler."""
    arr = scaled_df.withColumn("_z", vector_to_array(FEATURES_COL))
    aggs = []
    for i, c in enumerate(feature_cols):
        aggs += [F.avg(c).alias(f"{c}|raw_mean"), F.stddev(c).alias(f"{c}|raw_std"),
                 F.avg(F.col("_z")[i]).alias(f"{c}|scaled_mean"), F.stddev(F.col("_z")[i]).alias(f"{c}|scaled_std"),
                 F.min(F.col("_z")[i]).alias(f"{c}|scaled_min"), F.max(F.col("_z")[i]).alias(f"{c}|scaled_max")]
        if log_transform:
            aggs += [F.avg(f"log_{c}").alias(f"{c}|log_mean"), F.stddev(f"log_{c}").alias(f"{c}|log_std")]
    r = arr.agg(*aggs).first().asDict()
    return {
        c: {k.split("|")[1]: round(v, 4) for k, v in r.items() if k.startswith(f"{c}|")}
        for c in feature_cols
    }


def train_kmeans(scaled_df: DataFrame, k: int, seed: int = 42, max_iter: int = 100, tol: float = 1e-4) -> KMeansModel:
    """Huấn luyện K-Means với k cụm (khoảng cách Euclid, khởi tạo k-means||), trả về model."""
    return KMeans(
        k=k, seed=seed, maxIter=max_iter, tol=tol,
        featuresCol=FEATURES_COL, predictionCol=CLUSTER_COL,
    ).fit(scaled_df)


def cluster_rfm_means(pred: DataFrame, feature_cols: list[str]) -> list[dict]:
    """Số khách + trung bình R/F/M GỐC theo cụm (để xem cụm có diễn giải được không; không đặt tên)."""
    rows = (
        pred.groupBy(CLUSTER_COL)
        .agg(F.count("*").alias("customers"), *[F.round(F.avg(c), 2).alias(f"avg_{c}") for c in feature_cols])
        .orderBy(CLUSTER_COL).collect()
    )
    return [r.asDict() for r in rows]


def main() -> int:
    from src.clustering.evaluate import silhouette_score
    from src.utils.spark_session import get_spark

    cfg = load_config()
    cc = cfg["clustering"]
    k = cc.get("selected_k")
    if not k:
        print("ERROR: clustering.selected_k chưa được chọn. Chạy src/clustering/evaluate.py, xem kết quả, "
              "rồi điền selected_k trong configs/config.yaml")
        return 1
    features = cc["features"]
    customer_col = cfg["rfm"]["customer_col"]
    rfm_uri = hdfs_uri(cfg["hdfs"]["rfm_dir"])
    out_uri = hdfs_uri(f"{cfg['hdfs']['output_dir']}/clustering")
    report_path = PROJECT_ROOT / cfg["paths"]["output_reports"] / cc["kmeans_report"]

    spark = get_spark("phase6-kmeans")
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.perf_counter()

    print(f"Input : {rfm_uri}")
    rfm = hdfs_loader.read_parquet(spark, rfm_uri)
    scaled, _ = scale_features(rfm, features, cc["log_transform"])
    scaled = scaled.cache()
    n = scaled.count()
    scaling = scaling_summary(scaled, features, cc["log_transform"])
    print(f"customers={n:,}  log_transform={cc['log_transform']}  k={k}")
    for c, s in scaling.items():
        print(f"  {c:<10} {s}")

    model = train_kmeans(scaled, k, cc["seed"], cc["max_iter"], cc["tol"])
    pred = model.transform(scaled).cache()
    sil = silhouette_score(pred)
    summary = model.summary
    profile = cluster_rfm_means(pred, features)
    print(f"\nsilhouette={sil:.4f}  numIter={summary.numIter}/{cc['max_iter']}  WSSSE={summary.trainingCost:.2f}")
    print("Centroids (không gian đã scale):")
    for i, c in enumerate(model.clusterCenters()):
        print(f"  Cluster {i}: {[round(float(v), 4) for v in c]}")
    print("Cluster sizes + trung bình R/F/M gốc:")
    pred.groupBy(CLUSTER_COL).agg(
        F.count("*").alias("customers"), *[F.round(F.avg(c), 2).alias(f"avg_{c}") for c in features]
    ).orderBy(CLUSTER_COL).show()

    print(f"Output: {out_uri} (Parquet, overwrite)")
    out = pred.select(customer_col, *features, CLUSTER_COL)
    out.coalesce(cc["output_files"]).write.mode("overwrite").parquet(out_uri)
    files = [f for f in hdfs_loader.list_dir(spark, out_uri) if f["path"].endswith(".parquet")]

    report = {
        "input": rfm_uri,
        "customers": n,
        "features": features,
        "log_transform": cc["log_transform"],
        "scaler": "StandardScaler(withMean=True, withStd=True)",
        "scaling_summary": scaling,
        "k": k,
        "seed": cc["seed"],
        "max_iter": cc["max_iter"],
        "tol": cc["tol"],
        "init_mode": model.getInitMode(),
        "num_iter": summary.numIter,
        "converged": summary.numIter < cc["max_iter"],
        "wssse": round(summary.trainingCost, 4),
        "silhouette": round(sil, 4),
        "cluster_sizes": list(summary.clusterSizes),
        "centroids_scaled": [[round(float(v), 4) for v in c] for c in model.clusterCenters()],
        "cluster_rfm_means": profile,
        "output": {
            "hdfs_uri": out_uri,
            "parquet_files": len(files),
            "schema": [[f.name, f.dataType.simpleString()] for f in out.schema.fields],
        },
        "elapsed_seconds": round(time.perf_counter() - t0, 1),
    }
    pred.unpersist()
    scaled.unpersist()
    spark.stop()

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"elapsed={report['elapsed_seconds']}s  report -> {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
