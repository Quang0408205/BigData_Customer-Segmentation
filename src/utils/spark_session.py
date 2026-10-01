"""Tạo SparkSession dùng chung cho toàn project.

Master URL đọc từ biến môi trường SPARK_MASTER_URL (.env):
- local[*]               : Spark chạy trong 1 JVM trên máy hiện tại (Phase 1).
- spark://<host>:7077    : gửi job lên Spark cluster trong Docker (từ Phase 2).
"""

import os
import sys

from src.config.settings import load_config, load_env


def get_spark(app_name: str | None = None, master: str | None = None):
    from pyspark.sql import SparkSession

    load_env()
    cfg = load_config()

    # Worker Python phải cùng interpreter với driver (tránh lỗi version mismatch trên Windows)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder
        .appName(app_name or cfg["spark"]["app_name"])
        .master(master or os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        # Chỉ có tác dụng khi chạy trên cluster (local mode không có executor riêng)
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g"))
        # InvoiceDate là giờ địa phương, KHÔNG có timezone -> dùng TIMESTAMP_NTZ.
        # Với TIMESTAMP (có timezone) mặc định, collect() về Python bị đổi sang
        # giờ máy chạy (UTC+7) -> lệch 7 tiếng (đã gặp ở Phase 1).
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .config("spark.sql.session.timeZone", cfg["spark"].get("timezone", "UTC"))
        .getOrCreate()
    )
