"""Ingestion: đọc dữ liệu từ HDFS vào Spark DataFrame.

Spark nói chuyện với HDFS qua Hadoop FileSystem API (có sẵn trong Spark):
driver hỏi NameNode xem file gồm những block nào, nằm ở DataNode nào;
executor đọc trực tiếp từng block từ DataNode. Mỗi split (thường ~1 block)
thành một partition -> nhiều task đọc song song.
"""

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import hdfs_uri, load_config
from src.ingestion.csv_reader import read_raw_csv


def _jvm_fs(spark: SparkSession, uri: str):
    """Hadoop FileSystem (Java) ứng với uri, truy cập qua py4j."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(conf), path


def path_exists(spark: SparkSession, uri: str) -> bool:
    fs, path = _jvm_fs(spark, uri)
    return fs.exists(path)


def list_dir(spark: SparkSession, uri: str) -> list[dict]:
    """Liệt kê một thư mục HDFS: tên, kích thước, là thư mục hay file."""
    fs, path = _jvm_fs(spark, uri)
    return [
        {
            "path": s.getPath().toString(),
            "is_dir": s.isDirectory(),
            "bytes": s.getLen(),
            "replication": s.getReplication(),
            "block_size": s.getBlockSize(),
        }
        for s in fs.listStatus(path)
    ]


def block_locations(spark: SparkSession, uri: str) -> list[dict]:
    """Các block của một file HDFS: offset, độ dài, DataNode đang giữ block.

    Đây chính là thông tin driver hỏi NameNode trước khi chia file thành partition.
    """
    fs, path = _jvm_fs(spark, uri)
    status = fs.getFileStatus(path)
    return [
        {"offset": b.getOffset(), "length": b.getLength(), "hosts": list(b.getHosts())}
        for b in fs.getFileBlockLocations(status, 0, status.getLen())
    ]


def delete_path(spark: SparkSession, uri: str, recursive: bool = True) -> bool:
    fs, path = _jvm_fs(spark, uri)
    return fs.delete(path, recursive)


def raw_dataset_uri() -> str:
    cfg = load_config()
    return hdfs_uri(f"{cfg['hdfs']['raw_dir']}/{cfg['dataset']['raw_filename']}")


def read_raw_transactions(
    spark: SparkSession, uri: str | None = None, with_corrupt_record: bool = False
) -> DataFrame:
    """Đọc CSV raw từ HDFS (mặc định: file dataset trong hdfs.raw_dir).

    Đây là điểm vào duy nhất của pipeline từ Phase 3: các bước sau đọc raw
    qua hàm này (HDFS), không đọc data/raw/ trên máy local nữa.
    Upload file lên HDFS bằng scripts/upload_to_hdfs.py.
    """
    uri = uri or raw_dataset_uri()
    if not path_exists(spark, uri):
        raise FileNotFoundError(f"Not found on HDFS: {uri} (run scripts/upload_to_hdfs.py)")
    return read_raw_csv(spark, uri, with_corrupt_record)


def read_parquet(spark: SparkSession, uri: str) -> DataFrame:
    if not path_exists(spark, uri):
        raise FileNotFoundError(f"Not found on HDFS: {uri}")
    return spark.read.parquet(uri)
