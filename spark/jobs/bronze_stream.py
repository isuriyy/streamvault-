"""
bronze_stream.py — StreamVault Phase 2
Reads all 5 Olist topics from Redpanda via Structured Streaming
and writes raw events to Delta Lake bronze tables.

Bronze layer principles:
  - No transforms. Raw JSON preserved exactly as received from Kafka.
  - Append-only. Never update or delete bronze data.
  - Full audit trail. _ingested_at and _event_id from producer preserved.
  - Partitioned by ingestion date for efficient downstream reads.

Run:
    docker compose -f docker-compose.redpanda.yml run --rm spark bronze_stream.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json,
    to_date, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

# ── Config from environment (set in docker-compose) ──────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER",    "redpanda:9092")
LAKEHOUSE_PATH  = os.getenv("LAKEHOUSE_PATH",  "/lakehouse")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/checkpoints")

BRONZE_PATH     = f"{LAKEHOUSE_PATH}/bronze"

# ── Schemas ───────────────────────────────────────────────────────────────────
# Explicit schemas are better than inferring — faster and catches bad data early

SCHEMA_ORDERS = StructType([
    StructField("order_id",                        StringType()),
    StructField("customer_id",                     StringType()),
    StructField("order_status",                    StringType()),
    StructField("order_purchase_timestamp",        StringType()),
    StructField("order_approved_at",               StringType()),
    StructField("order_delivered_carrier_date",    StringType()),
    StructField("order_delivered_customer_date",   StringType()),
    StructField("order_estimated_delivery_date",   StringType()),
    StructField("_topic",                          StringType()),
    StructField("_ingested_at",                    StringType()),
    StructField("_event_id",                       StringType()),
])

SCHEMA_ORDER_ITEMS = StructType([
    StructField("order_id",             StringType()),
    StructField("order_item_id",        StringType()),
    StructField("product_id",           StringType()),
    StructField("seller_id",            StringType()),
    StructField("shipping_limit_date",  StringType()),
    StructField("price",                StringType()),
    StructField("freight_value",        StringType()),
    StructField("_topic",               StringType()),
    StructField("_ingested_at",         StringType()),
    StructField("_event_id",            StringType()),
])

SCHEMA_ORDER_PAYMENTS = StructType([
    StructField("order_id",              StringType()),
    StructField("payment_sequential",    StringType()),
    StructField("payment_type",          StringType()),
    StructField("payment_installments",  StringType()),
    StructField("payment_value",         StringType()),
    StructField("_topic",                StringType()),
    StructField("_ingested_at",          StringType()),
    StructField("_event_id",             StringType()),
])

SCHEMA_CUSTOMERS = StructType([
    StructField("customer_id",              StringType()),
    StructField("customer_unique_id",       StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city",            StringType()),
    StructField("customer_state",           StringType()),
    StructField("_topic",                   StringType()),
    StructField("_ingested_at",             StringType()),
    StructField("_event_id",                StringType()),
])

SCHEMA_PRODUCTS = StructType([
    StructField("product_id",                   StringType()),
    StructField("product_category_name",        StringType()),
    StructField("product_name_lenght",          StringType()),
    StructField("product_description_lenght",   StringType()),
    StructField("product_photos_qty",           StringType()),
    StructField("product_weight_g",             StringType()),
    StructField("product_length_cm",            StringType()),
    StructField("product_height_cm",            StringType()),
    StructField("product_width_cm",             StringType()),
    StructField("_topic",                       StringType()),
    StructField("_ingested_at",                 StringType()),
    StructField("_event_id",                    StringType()),
])

# ── Topic → (table name, schema) ─────────────────────────────────────────────
TOPIC_CONFIG = {
    "olist.orders":         ("orders",         SCHEMA_ORDERS),
    "olist.order_items":    ("order_items",     SCHEMA_ORDER_ITEMS),
    "olist.order_payments": ("order_payments",  SCHEMA_ORDER_PAYMENTS),
    "olist.customers":      ("customers",       SCHEMA_CUSTOMERS),
    "olist.products":       ("products",        SCHEMA_PRODUCTS),
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StreamVault-Bronze")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_topic(spark: SparkSession, topic: str):
    """Returns a streaming DataFrame reading from one Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")   # replay everything on first run
        .option("failOnDataLoss", "false")        # safe for dev — don't crash if offsets drift
        .load()
    )


def parse_and_enrich(raw_df, schema: StructType, table_name: str):
    """
    Parses the Kafka value (JSON bytes → struct),
    adds bronze audit columns, and adds a partition column.
    """
    parsed = (
        raw_df
        .select(
            # Kafka envelope fields — useful for debugging
            col("topic").alias("_kafka_topic"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
            # Parse JSON payload
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select(
            col("_kafka_topic"),
            col("_kafka_partition"),
            col("_kafka_offset"),
            col("_kafka_timestamp"),
            col("data.*")   # expand all fields from the JSON schema
        )
        # Partition column — used to organise files on disk efficiently
        .withColumn("_bronze_date", to_date(col("_ingested_at")))
    )
    return parsed


def write_bronze_table(df, table_name: str):
    """
    Writes a streaming DataFrame to a Delta Lake bronze table.
    Append-only, partitioned by ingestion date.
    Checkpoint per table so each stream resumes independently.
    """
    output_path     = f"{BRONZE_PATH}/{table_name}"
    checkpoint_path = f"{CHECKPOINT_PATH}/bronze/{table_name}"

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("_bronze_date")
        .trigger(processingTime="30 seconds")  # micro-batch every 30s
        .start()
    )


def main():
    print("StreamVault — Bronze streaming job starting")
    print(f"  Kafka broker  : {KAFKA_BROKER}")
    print(f"  Bronze path   : {BRONZE_PATH}")
    print(f"  Checkpoint    : {CHECKPOINT_PATH}")
    print()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    queries = []

    for topic, (table_name, schema) in TOPIC_CONFIG.items():
        print(f"  Starting stream: {topic} → bronze/{table_name}")

        raw_df    = read_kafka_topic(spark, topic)
        parsed_df = parse_and_enrich(raw_df, schema, table_name)
        query     = write_bronze_table(parsed_df, table_name)
        queries.append(query)

    print(f"\n  {len(queries)} streaming queries running.")
    print("  Writing to Delta Lake bronze layer...")
    print("  Press Ctrl+C to stop.\n")

    # Wait for all streams — runs until interrupted
    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        print("\nShutting down streams...")
        for q in queries:
            q.stop()
        spark.stop()
        print("Done.")


if __name__ == "__main__":
    main()
