"""Kiểm tra Phase 3: Local Dataset -> HDFS -> Spark DataFrame -> Schema -> Sample Records.

Chạy BÊN TRONG container spark-master, sau khi đã chạy scripts/upload_to_hdfs.py:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 scripts/check_hdfs_ingestion.py

Spark đọc file raw trực tiếp từ HDFS (hdfs_loader.read_raw_transactions).
File local (bind mount /opt/project/data/raw) chỉ được đọc để ĐỐI CHIẾU vài dòng đầu,
không dùng làm input. Số liệu kỳ vọng lấy từ kết quả thật của Phase 1:
    - data/raw/online_retail_II.metadata.json    (bytes, rows của CSV)
    - output/reports/dataset_profile.json        (missing, corrupt record)

Kết quả ghi ra output/reports/hdfs_ingestion_check.json để các Phase sau dùng thống nhất.
Exit code 0 = PASS, 1 = FAIL.
"""

import csv
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType  # noqa: E402

from src.config.settings import PROJECT_ROOT, load_config  # noqa: E402
from src.ingestion import hdfs_loader  # noqa: E402
from src.ingestion.csv_reader import CORRUPT_COL, RAW_COLUMNS  # noqa: E402
from src.utils.spark_session import get_spark  # noqa: E402

N_SAMPLE = 5


def step(n: int, title: str) -> None:
    print(f"\n[{n}] {title}\n" + "-" * 70)


def load_json(path: Path) -> dict | None:
    if not path.is_file():
        print(f"WARN: {path} not found -> skip this comparison")
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def local_first_rows(path: Path, n: int) -> list[dict]:
    """n dòng dữ liệu đầu của CSV local (giá trị rỗng -> None, giống Spark đọc)."""
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [{k: (v if v != "" else None) for k, v in row.items()} for _, row in zip(range(n), reader)]


def main() -> int:
    cfg = load_config()
    local_csv = PROJECT_ROOT / cfg["paths"]["raw"] / cfg["dataset"]["raw_filename"]
    metadata = load_json(PROJECT_ROOT / cfg["paths"]["raw"] / cfg["dataset"]["metadata_filename"])
    profile = load_json(PROJECT_ROOT / cfg["paths"]["output_reports"] / "dataset_profile.json")
    uri = hdfs_loader.raw_dataset_uri()
    checks: dict[str, bool] = {}

    spark = get_spark("phase3-hdfs-ingestion-check")
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    step(1, f"HDFS file: {uri}")
    if not hdfs_loader.path_exists(spark, uri):
        print("ERROR: file not found on HDFS -> run: python scripts/upload_to_hdfs.py")
        spark.stop()
        return 1
    status = hdfs_loader.list_dir(spark, uri)[0]
    blocks = hdfs_loader.block_locations(spark, uri)
    print(f"bytes={status['bytes']:,}  block_size={status['block_size']:,}  replication={status['replication']}")
    for i, b in enumerate(blocks):
        print(f"  block {i}: offset={b['offset']:,} length={b['length']:,} datanodes={b['hosts']}")
    if metadata:
        checks["hdfs_bytes_match_local_metadata"] = status["bytes"] == metadata["csv"]["bytes"]

    step(2, "Spark DataFrame from HDFS -> schema")
    df = hdfs_loader.read_raw_transactions(spark, with_corrupt_record=True)
    df.printSchema()
    data_cols = [f for f in df.schema.fields if f.name != CORRUPT_COL]
    checks["columns_match_raw"] = [f.name for f in data_cols] == RAW_COLUMNS
    checks["all_columns_string"] = all(isinstance(f.dataType, StringType) for f in data_cols)

    step(3, "Record count + partitions")
    # cache: Spark không cho query riêng cột _corrupt_record trên file CSV chưa cache
    df.cache()
    t0 = time.perf_counter()
    rows = df.count()
    read_seconds = round(time.perf_counter() - t0, 2)
    partitions = df.rdd.getNumPartitions()
    conf = spark.conf
    print(f"records={rows:,}  columns={len(data_cols)}  (read+count {read_seconds}s, includes caching)")
    print(f"partitions={partitions}  defaultParallelism={sc.defaultParallelism}  "
          f"maxPartitionBytes={conf.get('spark.sql.files.maxPartitionBytes')}  "
          f"openCostInBytes={conf.get('spark.sql.files.openCostInBytes')}")
    if metadata:
        checks["record_count_match_local_metadata"] = rows == metadata["csv"]["rows"]

    step(4, "Corrupt records + null per column (đối chiếu Phase 1, không xử lý)")
    corrupt = df.filter(F.col(CORRUPT_COL).isNotNull()).count()
    nulls = df.select([F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(c) for c in RAW_COLUMNS]).first().asDict()
    print(f"corrupt_records={corrupt:,}")
    for c in RAW_COLUMNS:
        print(f"  null {c:<12} {nulls[c]:>9,}")
    checks["no_corrupt_records"] = corrupt == 0
    if profile:
        checks["nulls_match_phase1_profile"] = all(nulls[c] == profile["missing"][c]["null"] for c in RAW_COLUMNS)

    step(5, f"Sample records (first {N_SAMPLE}) — HDFS vs local CSV")
    df.select(RAW_COLUMNS).show(N_SAMPLE, truncate=False)
    hdfs_rows = [r.asDict() for r in df.select(RAW_COLUMNS).take(N_SAMPLE)]
    if local_csv.is_file():
        local_rows = local_first_rows(local_csv, N_SAMPLE)
        checks["first_rows_match_local_csv"] = hdfs_rows == local_rows
    else:
        print(f"WARN: {local_csv} not found -> skip sample comparison")
    df.unpersist()

    step(6, "Checks")
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    passed = all(checks.values())

    report = {
        "hdfs_uri": uri,
        "hdfs_file": {**status, "blocks": blocks},
        "schema": [[f.name, f.dataType.simpleString()] for f in data_cols],
        "records": rows,
        "columns": len(data_cols),
        "partitions": partitions,
        "default_parallelism": sc.defaultParallelism,
        "max_partition_bytes": conf.get("spark.sql.files.maxPartitionBytes"),
        "read_count_seconds": read_seconds,
        "corrupt_records": corrupt,
        "nulls": nulls,
        "sample_first_rows": hdfs_rows,
        "checks": checks,
        "result": "PASS" if passed else "FAIL",
        "spark_version": spark.version,
        "master": sc.master,
    }
    out = PROJECT_ROOT / cfg["paths"]["output_reports"] / "hdfs_ingestion_check.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nreport -> {out}")

    print("\nRESULT:", "PASS" if passed else "FAIL")
    spark.stop()
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
