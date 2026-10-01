"""Đọc file CSV raw Online Retail II bằng Spark.

Path có thể là local (file:///...) hoặc HDFS (hdfs://namenode:8020/...):
cùng một hàm dùng được cho cả Phase 1 (local) và các Phase sau (HDFS).

Tất cả cột được đọc dạng STRING: tầng raw giữ nguyên giá trị gốc,
việc ép kiểu và xử lý giá trị lỗi thuộc về preprocessing.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

RAW_COLUMNS = [
    "Invoice", "StockCode", "Description", "Quantity",
    "InvoiceDate", "Price", "Customer ID", "Country",
]
CORRUPT_COL = "_corrupt_record"


def raw_schema(with_corrupt_record: bool = False) -> StructType:
    fields = [StructField(c, StringType(), True) for c in RAW_COLUMNS]
    if with_corrupt_record:
        fields.append(StructField(CORRUPT_COL, StringType(), True))
    return StructType(fields)


def read_raw_csv(spark: SparkSession, path: str, with_corrupt_record: bool = False) -> DataFrame:
    """Đọc CSV raw với schema tường minh (toàn bộ STRING).

    with_corrupt_record=True: dòng không parse được (sai số cột, lỗi quote)
    được giữ lại, nội dung gốc nằm ở cột _corrupt_record.
    """
    header = spark.read.option("header", True).csv(path).columns
    if header != RAW_COLUMNS:
        raise ValueError(f"Unexpected CSV header: {header}, expected {RAW_COLUMNS}")

    reader = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        # CSV do pandas ghi theo chuẩn RFC 4180: dấu " trong text được escape thành "".
        # Mặc định Spark dùng escape '\', nên phải khai báo lại.
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
    )
    if with_corrupt_record:
        reader = reader.option("columnNameOfCorruptRecord", CORRUPT_COL)
    return reader.schema(raw_schema(with_corrupt_record)).csv(path)
