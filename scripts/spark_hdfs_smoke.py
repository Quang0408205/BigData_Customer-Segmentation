"""Smoke test Phase 2: Spark cluster chạy được và đọc/ghi được HDFS.

Chạy BÊN TRONG container spark-master (driver cùng Docker network với HDFS):

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 scripts/spark_hdfs_smoke.py

Các bước:
    1. In version Spark / PySpark / Python, master URL, số executor.
    2. Chạy một job tính toán nhỏ trên cluster.
    3. Liệt kê thư mục project trên HDFS.
    4. Đọc file CSV từ HDFS -> schema, count, sample.
    5. Ghi Parquet vào HDFS, đọc lại, rồi xóa (kiểm tra quyền ghi của user spark).
"""

import argparse
import platform
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyspark  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.config.settings import hdfs_uri, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402

DEFAULT_INPUT = "/tmp/phase2-smoke/online_retail_II_sample.csv"


def step(n: int, title: str) -> None:
    print(f"\n[{n}] {title}\n" + "-" * 60)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT, help="đường dẫn HDFS của file CSV để đọc thử")
    args = parser.parse_args()
    cfg = load_config()["hdfs"]

    spark = get_spark("phase2-smoke")
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    step(1, "Versions & cluster")
    print(f"Spark version     : {spark.version}")
    print(f"PySpark version   : {pyspark.__version__}")
    print(f"Python (driver)   : {platform.python_version()}")
    print(f"Master            : {sc.master}")
    print(f"dfs.replication (client): {sc._jsc.hadoopConfiguration().get('dfs.replication')}")

    step(2, "Distributed job on cluster")
    total = spark.range(0, 10_000_000, numPartitions=8).agg(F.sum("id")).first()[0]
    print(f"sum(0..9,999,999) = {total:,}  (expected {sum(range(10_000_000)):,})")
    py_versions = (
        sc.parallelize(range(4), 4).map(lambda _: platform.python_version()).distinct().collect()
    )
    print(f"Python on executors: {py_versions}")
    # Đo sau khi job chạy: lúc vừa tạo SparkSession executor có thể chưa kịp đăng ký.
    # getExecutorMemoryStatus gồm cả driver -> trừ 1
    print(f"Executors          : {sc._jsc.sc().getExecutorMemoryStatus().size() - 1}")
    print(f"Default parallelism: {sc.defaultParallelism} (= tổng số core của executor)")

    step(3, f"List HDFS {cfg['base_dir']}")
    for item in hdfs_loader.list_dir(spark, hdfs_uri(cfg["base_dir"])):
        print(f"  {'DIR ' if item['is_dir'] else 'FILE'} {item['path']}")

    step(4, f"Read CSV from HDFS: {args.input}")
    uri = hdfs_uri(args.input)
    df = hdfs_loader.read_raw_transactions(spark, uri)
    df.printSchema()
    print(f"rows={df.count():,}  partitions={df.rdd.getNumPartitions()}")
    df.show(5, truncate=False)

    step(5, "Write Parquet to HDFS, read back, clean up")
    # Không đặt tên bắt đầu bằng "_" hoặc ".": Spark coi là file ẩn (như _SUCCESS)
    out = hdfs_uri(f"{cfg['output_dir']}/smoke_parquet")
    df.write.mode("overwrite").parquet(out)
    files = [i for i in hdfs_loader.list_dir(spark, out) if i["path"].endswith(".parquet")]
    back = hdfs_loader.read_parquet(spark, out).count()
    print(f"wrote {len(files)} parquet file(s), replication={files[0]['replication'] if files else '-'}, "
          f"read back rows={back:,}")
    print(f"deleted: {hdfs_loader.delete_path(spark, out)}")

    ok = back == df.count() and total == sum(range(10_000_000))
    print("\nRESULT:", "PASS" if ok else "FAIL")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
