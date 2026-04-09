import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
from config.settings import (
    BUCKET_AGGREGATED,
    BUCKET_INVALID,
    BUCKET_PROCESSED,
    KAFKA_BOOTSTRAP_SERVERS,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    SPARK_APP_NAME,
    SPARK_CHECKPOINT_DIR,
    SPARK_MASTER,
    SPARK_TRIGGER_INTERVAL,
    TOPIC_ALERTS,
    TOPIC_PROCESSED,
    TOPIC_RAW,
)
from spark_streaming.transformation import (
    clean_events,
    enrich_events,
    parse_kafka_payload,
)
from spark_streaming.analytics import (
    avg_aqi_per_zone,
    traffic_congestion,
    temperature_trends,
    noise_levels,
    active_sensors_counts,
    event_thorughput,
)
from spark_streaming.alert_engine import detect_all_alerts

log = logging.getLogger("stream_processor")
logging.basicConfig(level=logging.INFO)

# Spark session factory

def create_spark_session() -> SparkSession:
    """
    Build and return a SparkSession configured for:
        Kafka
        Minio (S3A)
        Hadoop S3A committer for reliable parquet writes
    """

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)

        # Minio settings

        .config("spark.hadoop.fs.s3a.endpoint",  f"http://{MINIO_ENDPOINT}")
        .config("spsrk.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Use staging committer for reliable atomic writes to object storage
    
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.sql.streaming.schemaInference", "true")

        # Checkpointing

        .config("spark.sql.streaming.checkpointLocation", SPARK_CHECKPOINT_DIR)
            .getOrCreate()
     )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# Kafka sink helper

def _to_kafka(df, topic: str, checkpoint_suffix: str):
    """
    Write a DataFrame to a Kafka topic in append mode.
    The entire row is JSON-serialised into the Kafka ``value`` column.
    The ``sensor_id`` (or ``alert_id``) is used as the message key.
    """
    key_col = "sensor_id" if "sensor_id" in df.columns else "alert_id"

    return (
        df.select(
            F.col(key_col).cast("string").alias("key"),
            F.to_json(F.struct(*df.columns)).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/{checkpoint_suffix}")
        .outputMode("append")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start()
    )

#  MinIO / S3A sink helper

def _to_minio(df, bucket: str, prefix: str, checkpoint_suffix: str,
              partition_cols=("year", "month", "day")):
    """
    Write a DataFrame to MinIO as Parquet, partitioned by year/month/day.
    Uses append output mode and a file-sink for durability.
    """
    path = f"s3a://{bucket}/{prefix}/"
    return (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/{checkpoint_suffix}")
        .outputMode("append")
        .partitionBy(*partition_cols)
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start()
    )

# Aggregation sink helper

def _agg_to_minio(df, bucket: str, prefix: str, checkpoint_suffix: str):
    """Write a windowed aggregation DataFrame to MinIO."""
    path = f"s3a://{bucket}/{prefix}/"
    return (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/{checkpoint_suffix}")
        .outputMode("complete")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start()
    )

# Main entry-point

def main():
    spark = create_spark_session()
    log.info("SparkSession created: %s", SPARK_APP_NAME)

    # 1. Read raw  events from Kafka
    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 100_000)   # back-pressure: 100k/trigger
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON payload
    parsed_df = parse_kafka_payload(raw_kafka_df)

    # 3. Clean & split
    valid_df, invalid_df = clean_events(parsed_df)

    # 4. Enrich valid events
    enriched_df = enrich_events(valid_df)

    # 5. Detect alerts
    alerts_df = detect_all_alerts(enriched_df)

    # 6. Compute window aggregations
    aqi_df      = avg_aqi_per_zone(enriched_df)
    traffic_df  = traffic_congestion(enriched_df)
    temp_df     = temperature_trends(enriched_df)
    noise_df    = noise_levels(enriched_df)
    counts_df   = active_sensor_counts(enriched_df)
    thruput_df  = event_throughput(enriched_df)

    # 7. Prepare invalid records for MinIO (Add time-partition columns so invalid data lands in the same layout)
    invalid_enriched = (
        invalid_df
        .withColumn("year",  F.year(F.current_timestamp()).cast("string"))
        .withColumn("month", F.lpad(F.month(F.current_timestamp()).cast("string"), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth(F.current_timestamp()).cast("string"), 2, "0"))
    )

    # 8. Start all streaming queries
    queries = []

    queries.append(
        _to_kafka(enriched_df, TOPIC_PROCESSED, "iot_processed_kafka")    # valid events moving to Kafka iot_sensor_processed
    )
    log.info("Started query: valid events data moving to Kafka processed topic")

    queries.append(
        _to_kafka(alerts_df,  TOPIC_PROCESSED, "iot_alerts_kafka")        # alerts moving to Kafka iot_alerts
    )
    log.info("Started query: alerts data moving to Kafka alerts topic")

    queries.append(
        _to_minio(
            enriched_df.drop("year", "month", "day"),                     # keep non-partition cols clean
            BUCKET_PROCESSED, "events",                                   # valid events moving to MinIO processed as Parquet
            "iot_processed_minio",
        )
    )
    log.info("Started query: valid events moving to MinIO processed as parquet")

    # Aggregations data moving to MinIO aggregated
    agg_sinks = [
        (aqi_df,     "aqi",          "ckpt_agg_aqi"),
        (traffic_df, "traffic",      "ckpt_agg_traffic"),
        (temp_df,    "temperature",  "ckpt_agg_temp"),
        (noise_df,   "noise",        "ckpt_agg_noise"),
        (counts_df,  "sensor_counts","ckpt_agg_counts"),
        (thruput_df, "throughput",   "ckpt_agg_thruput"),
    ]
    for agg_df, prefix, ckpt in agg_sinks:
        queries.append(
            _agg_to_minio(agg_df, BUCKET_AGGREGATED, prefix, ckpt)
        )
        log.info("Started aggregation query: %s → MinIO aggregated/%s", prefix, prefix)

    log.info("All %d streaming queries active. Awaiting termination…", len(queries))

    # Keep the driver alive until all queries terminate or fail
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()