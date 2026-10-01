# Spark image cho master, worker và driver (spark-submit chạy trong container).
# Version 4.0.4 phải trùng với pyspark==4.0.4 trong requirements.txt.
FROM apache/spark:4.0.4-java21-python3

USER root
# Thư viện Python mà code trong src/ cần khi chạy trên cluster.
# numpy: bắt buộc cho pyspark.ml (Spark MLlib: VectorAssembler, StandardScaler, KMeans) - Phase 6.
RUN pip3 install --no-cache-dir "pyyaml>=6.0" "python-dotenv>=1.0" "numpy>=1.21"
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark
