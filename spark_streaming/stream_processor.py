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