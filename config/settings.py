# config/settings.py
"""
Centralized configuration for the Smart City IoT Streaming Pipeline.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple
import os

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_RAW       = "iot_sensor_raw"
TOPIC_PROCESSED = "iot_sensor_processed"
TOPIC_ALERTS    = "iot_alerts"

KAFKA_NUM_PARTITIONS = 12
KAFKA_REPLICATION    = 1

# ---------------------------------------------------------------------------
# MinIO / S3-compatible object store
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = False

BUCKET_RAW        = "smart-city-raw"
BUCKET_PROCESSED  = "smart-city-processed"
BUCKET_INVALID    = "smart-city-invalid"
BUCKET_AGGREGATED = "smart-city-aggregated"

ALL_BUCKETS = [BUCKET_RAW, BUCKET_PROCESSED, BUCKET_INVALID, BUCKET_AGGREGATED]

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
SPARK_APP_NAME         = "SmartCityStreamProcessor"
SPARK_MASTER           = os.getenv("SPARK_MASTER", "local[*]")
SPARK_CHECKPOINT_DIR   = "/tmp/spark_checkpoints"
SPARK_WATERMARK_DELAY  = "30 seconds"
SPARK_TRIGGER_INTERVAL = "10 seconds"

SPARK_KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.kafka:kafka-clients:3.5.0"
)

# ---------------------------------------------------------------------------
# IoT Simulator
# ---------------------------------------------------------------------------
TARGET_EVENTS_PER_CYCLE = 500_000    # events per 2-minute window
CYCLE_SECONDS           = 120        # 2-minute cycle
BATCH_PRODUCE_SIZE      = 1_000      # records per Kafka produce call

# City spatial bounds (Lagos-inspired)
CITY_LAT_RANGE: Tuple[float, float] = (6.40, 6.70)
CITY_LON_RANGE: Tuple[float, float] = (3.25, 3.55)

CITY_ZONES: List[str] = [
    "Zone-Alpha", "Zone-Beta", "Zone-Gamma", "Zone-Delta",
    "Zone-Epsilon", "Zone-Zeta", "Zone-Eta", "Zone-Theta",
]

# ---------------------------------------------------------------------------
# Sensor definitions
# ---------------------------------------------------------------------------
@dataclass
class SensorSpec:
    sensor_type:     str
    count:           int
    reading_range:   Tuple[float, float]
    unit:            str
    alert_threshold: float = 0.0


SENSOR_SPECS: Dict[str, SensorSpec] = {
    "traffic": SensorSpec(
        sensor_type="traffic",
        count=2_000,
        reading_range=(0.0, 100.0),
        unit="vehicles/min",
        alert_threshold=80.0,
    ),
    "air_quality": SensorSpec(
        sensor_type="air_quality",
        count=1_500,
        reading_range=(0.0, 200.0),
        unit="AQI",
        alert_threshold=120.0,
    ),
    "noise": SensorSpec(
        sensor_type="noise",
        count=1_200,
        reading_range=(30.0, 120.0),
        unit="dB",
        alert_threshold=90.0,
    ),
    "weather": SensorSpec(
        sensor_type="weather",
        count=800,
        reading_range=(15.0, 45.0),
        unit="°C",
        alert_threshold=42.0,
    ),
    "energy": SensorSpec(
        sensor_type="energy",
        count=500,
        reading_range=(0.0, 500.0),
        unit="kWh",
        alert_threshold=450.0,
    ),
}

VALID_SENSOR_TYPES: List[str] = list(SENSOR_SPECS.keys())

# Battery alert threshold (applies to ALL sensor types)
BATTERY_ALERT_THRESHOLD = 15.0   # percent

# ---------------------------------------------------------------------------
# Great Expectations
# ---------------------------------------------------------------------------
GE_EXPECTATION_SUITE = "iot_sensor_suite"
GE_DATASOURCE_NAME   = "iot_kafka_source"

# ---------------------------------------------------------------------------
# Prometheus / Grafana exporter
# ---------------------------------------------------------------------------
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))