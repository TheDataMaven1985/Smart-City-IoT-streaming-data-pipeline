"""
Detects anomalous conditions in the clean event stream and emits structured
alert records.  Alerts are written to the ``iot_alerts`` Kafka topic and can
be consumed by the dashboard, PagerDuty webhook, or any downstream system.

Alert types:
    BATTERY_LOW -> battery_level < 15 %
    AQI_HIGH    -> air_quality reading > 120 (unhealthy AQI)
    NOISE_HIGH  -> noise reading > 90 dB (hearing-damage threshold)
    TRAFFIC_SPIKE -> traffic reading > 80 vehicles/min
    TEMP_EXTREME -> weather reading > 42 °C
    ENERGY_OVERLOAD -> energy reading > 450 kWh
"""

import os
import sys
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, TimestampType, StructType, StructField

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
from config.settings import (
    BATTERY_ALERT_THRESHOLD,
    SENSOR_SPECS,
)

# Alert Schema

ALERT_SCHEMA = StructType(
    [
        StructField("alert_id",     StringType(),    nullable=False),
        StructField("alert_type",   StringType(),    nullable=False),
        StructField("severity",     StringType(),    nullable=False),  # LOW / MED / HIGH / CRITICAL
        StructField("sensor_id",    StringType(),    nullable=False),
        StructField("sensor_type",  StringType(),    nullable=True),
        StructField("city_zone",    StringType(),    nullable=True),
        StructField("reading",      DoubleType(),    nullable=True),
        StructField("threshold",    DoubleType(),    nullable=True),
        StructField("event_time",   TimestampType(), nullable=True),
        StructField("alert_time",   TimestampType(), nullable=False),
        StructField("message",      StringType(),    nullable=True),
    ]
)

# UDF: generate a UUID for each alert row

_uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

# Per-alert-type detectors

def detect_battery_alerts(df: DataFrame) -> DataFrame:
    """
    Emit BATTERY_LOW alerts for any sensor below the threshold.
    Severity:
      CRITICAL < 5 %
      HIGH     5-10 %
      MEDIUM   10-15 %
    """
    filtered = df.filter(F.col("battery_level") < F.lit(BATTERY_ALERT_THRESHOLD))

    return filtered.select(
        _uuid_udf().alias("alert_id"),
        F.lit("BATTERY_LOW").alias("alert_type"),
        F.when(F.col("battery_level") < 5,  F.lit("CRITICAL"))
         .when(F.col("battery_level") < 10, F.lit("HIGH"))
         .otherwise(F.lit("MEDIUM"))
         .alias("severity"),
        "sensor_id",
        "sensor_type",
        "city_zone",
        F.col("battery_level").alias("reading"),
        F.lit(BATTERY_ALERT_THRESHOLD).cast(DoubleType()).alias("threshold"),
        "event_time",
        F.current_timestamp().alias("alert_time"),
        F.concat(
            F.lit("Battery at "),
            F.col("battery_level").cast("string"),
            F.lit("% on "),
            F.col("sensor_id"),
        ).alias("message"),
    )

def detect_aqi_alerts(df: DataFrame) -> DataFrame:
    """Emit AQI_HIGH when air-quality index exceeds threshold (120 AQI)."""
    threshold = SENSOR_SPECS["air_quality"].alert_threshold

    return (
        df.filter(
            (F.col("sensor_type") == "air_quality") &
            (F.col("reading") > F.lit(threshold))
        )
        .select(
            _uuid_udf().alias("alert_id"),
            F.lit("AQI_HIGH").alias("alert_type"),
            F.when(F.col("reading") > 200, F.lit("CRITICAL"))
             .when(F.col("reading") > 150, F.lit("HIGH"))
             .otherwise(F.lit("MEDIUM"))
             .alias("severity"),
            "sensor_id",
            "sensor_type",
            "city_zone",
            "reading",
            F.lit(threshold).cast(DoubleType()).alias("threshold"),
            "event_time",
            F.current_timestamp().alias("alert_time"),
            F.concat(
                F.lit("AQI="), F.col("reading").cast("string"),
                F.lit(" in "), F.col("city_zone"),
            ).alias("message"),
        )
    )

def detect_noise_alerts(df: DataFrame) -> DataFrame:
    """Emit NOISE_HIGH when noise exceeds 90 dB."""
    threshold = SENSOR_SPECS["noise"].alert_threshold

    return (
        df.filter(
            (F.col("sensor_type") == "noise") &
            (F.col("reading") > F.lit(threshold))
        )
        .select(
            _uuid_udf().alias("alert_id"),
            F.lit("NOISE_HIGH").alias("alert_type"),
            F.when(F.col("reading") > 110, F.lit("CRITICAL"))
             .when(F.col("reading") > 100, F.lit("HIGH"))
             .otherwise(F.lit("MEDIUM"))
             .alias("severity"),
            "sensor_id",
            "sensor_type",
            "city_zone",
            "reading",
            F.lit(threshold).cast(DoubleType()).alias("threshold"),
            "event_time",
            F.current_timestamp().alias("alert_time"),
            F.concat(
                F.lit("Noise="), F.col("reading").cast("string"),
                F.lit(" dB in "), F.col("city_zone"),
            ).alias("message"),
        )
    )

def detect_traffic_alerts(df: DataFrame) -> DataFrame:
    """Emit TRAFFIC_SPIKE when vehicle count exceeds threshold."""
    threshold = SENSOR_SPECS["traffic"].alert_threshold

    return (
        df.filter(
            (F.col("sensor_type") == "traffic") &
            (F.col("reading") > F.lit(threshold))
        )
        .select(
            _uuid_udf().alias("alert_id"),
            F.lit("TRAFFIC_SPIKE").alias("alert_type"),
            F.when(F.col("reading") > 95, F.lit("CRITICAL"))
             .when(F.col("reading") > 90, F.lit("HIGH"))
             .otherwise(F.lit("MEDIUM"))
             .alias("severity"),
            "sensor_id",
            "sensor_type",
            "city_zone",
            "reading",
            F.lit(threshold).cast(DoubleType()).alias("threshold"),
            "event_time",
            F.current_timestamp().alias("alert_time"),
            F.concat(
                F.lit("Traffic="), F.col("reading").cast("string"),
                F.lit(" vehicles/min in "), F.col("city_zone"),
            ).alias("message"),
        )
    )

def detect_temp_alerts(df: DataFrame) -> DataFrame:
    """Emit TEMP_EXTREME when temperature exceeds 42 °C."""
    threshold = SENSOR_SPECS["weather"].alert_threshold

    return (
        df.filter(
            (F.col("sensor_type") == "weather") &
            (F.col("reading") > F.lit(threshold))
        )
        .select(
            _uuid_udf().alias("alert_id"),
            F.lit("TEMP_EXTREME").alias("alert_type"),
            F.lit("HIGH").alias("severity"),
            "sensor_id",
            "sensor_type",
            "city_zone",
            "reading",
            F.lit(threshold).cast(DoubleType()).alias("threshold"),
            "event_time",
            F.current_timestamp().alias("alert_time"),
            F.concat(
                F.lit("Temp="), F.col("reading").cast("string"),
                F.lit("°C in "), F.col("city_zone"),
            ).alias("message"),
        )
    )


def detect_energy_alerts(df: DataFrame) -> DataFrame:
    """Emit ENERGY_OVERLOAD when energy reading exceeds 450 kWh."""
    threshold = SENSOR_SPECS["energy"].alert_threshold

    return (
        df.filter(
            (F.col("sensor_type") == "energy") &
            (F.col("reading") > F.lit(threshold))
        )
        .select(
            _uuid_udf().alias("alert_id"),
            F.lit("ENERGY_OVERLOAD").alias("alert_type"),
            F.when(F.col("reading") > 490, F.lit("CRITICAL"))
             .otherwise(F.lit("HIGH"))
             .alias("severity"),
            "sensor_id",
            "sensor_type",
            "city_zone",
            "reading",
            F.lit(threshold).cast(DoubleType()).alias("threshold"),
            "event_time",
            F.current_timestamp().alias("alert_time"),
            F.concat(
                F.lit("Energy="), F.col("reading").cast("string"),
                F.lit(" kWh at "), F.col("sensor_id"),
            ).alias("message"),
        )
    )

# Unified alert detector

def detect_all_alerts(clean_df: DataFrame) -> DataFrame:
    detectors = [
        detect_battery_alerts,
        detect_aqi_alerts,
        detect_noise_alerts,
        detect_traffic_alerts,
        detect_temp_alerts,
        detect_energy_alerts,
    ]

    # Union all alert DataFrames
    alert_df = None
    for detector in detectors:
        alerts = detector(clean_df)
        if alert_df is None:
            alert_df = alerts
        else:
            alert_df = alert_df.unionByName(alerts)

    return alert_df