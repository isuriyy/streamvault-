FROM bitnami/spark:3.5

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    delta-spark==3.2.0 \
    pyspark==3.5.0

# Download Kafka + Delta Lake JARs into Spark's jars directory
# These are needed for readStream from Kafka and writing Delta tables
RUN curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
    -o /opt/bitnami/spark/jars/kafka-clients-3.4.0.jar && \
    curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar && \
    curl -fsSL \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    -o /opt/bitnami/spark/jars/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    -o /opt/bitnami/spark/jars/delta-storage-3.2.0.jar

WORKDIR /app

ENTRYPOINT ["spark-submit", \
    "--master", "local[2]", \
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
    "--conf", "spark.sql.shuffle.partitions=4", \
    "--conf", "spark.databricks.delta.retentionDurationCheck.enabled=false"]
