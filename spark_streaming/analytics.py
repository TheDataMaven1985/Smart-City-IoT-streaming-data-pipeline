"""
Real Time Analytics

Aggregations Implemented:
    1. avg_aqi_per_zone: Average AQI index per city zone (5-min window)
    2. traffic_congestion: Vehicle count metrics per zone (2-min window)
    3. temperature_trends: Min / avg / max temperature per zone (10-min window)
    4. noise_levels_per_district: Avg / max noise dB per zone (2-min window)
    5. active_sensor_counts: Distinct active sensors per type (1-min window)
    6. throughput_metrics: Events-per-second across all sensors (30-sec window)
"""

import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
from config.settings import SPARK_WATERMARK_DELAY


# Helper: apply watermark once and re-use

def _with_watermark(df: DataFrame) -> DataFrame:
    """Apply watermark on event_time for laet data handling."""
    return df.withWatermark("event_time", SPARK_WATERMARK_DELAY)


# 1. Average AQI per city zone

def avg_aqi_per_zone(df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """
    Compute average Air Quality Index (AQI) per city zone within a tumbling
    window.  Only processes events where sensor_type == 'air_quality'.

    Expected output columns:
        window_start
        window_end
        city_zone
        avg_aqi
        min_aqi
        max_aqi
        event_count
    """
    return(
        _with_watermark(df)
        .filter(F.col("sensor_type") == "air_quality")
        .groupBy(
            F.window("event_time", window_duration).alias("window"),
            F.col("city_zone"),
        )
        .agg(
            F.avg("reading").cast(DoubleType()).alias("avg_aqi"),
            F.min("reading").cast(DoubleType()).alias("min_aqi"),
            F.max("reading").cast(DoubleType()).alias("max_aqi"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city_zone",
            "avg_aqi",
            "min_aqi",
            "max_aqi",
            "event_count",
            F.lit("aqi").alias("metric_type")
        )
    )

# 2. Traffic congestion metrics

def traffic_congestion(df: DataFrame, window_duration: str = "2 minutes") -> DataFrame:
    """
    Aggregate vehicle counts per city zone to derive congestion scores.

    Congestion score = avg_reading / max_reading (0-1 normalised).
    If max_reading == 0 the score defaults to 0.

    Expected output columns:
        window_start
        window_end
        city_zone
        avg_vehicle_count
        max_vehicle_count
        congestion_score
        event_count
    """

    agg_df = (
        _with_watermark(df)
        .filter(F.col("sensor_type") == "traffic")
        .groupBy(
            F.window("event_time", window_duration).alias("window"),
            F.col("city_zone"),      
        )
        .agg(
            F.avg("reading").cast(DoubleType()).alias("avg_vehicle_count"),
            F.max("reading").cast(DoubleType()).alias("max_vehicle_count"),
            F.count("*").alias("event_count"),
        )
    )

    # Compute normalised congestion score
    return agg_df.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "city_zone",
        "avg_vehicle_count",
        "max_vehicle_count",
        F.when(
            F.col("max_vehicle_count") > 0,
            F.col("avg_vehicle_count") / F.col("max_vehicle_count"),
        )
        .otherwise(F.lit(0.0))
        .cast(DoubleType())
        .alias("congestion_score"),
        "event_count",
        F.lit("traffic").alias("metric_type"),
    )


# 3. Temperature trends

def temperature_trends(df: DataFrame, window_duration: str = "10 minutes") -> DataFrame:
    """
    min / average / max temperature per city zone over a 10 minute window.

    Expected output columns:
        window_start
        window_end
        min_temp
        avg_temp
        max_temp
        event_count
    """

    return (
        _with_watermark(df)
        .filter(F.col("sensor_type") == "weather")
        .groupBy(
            F.window("event_time" window_duration).alias("window"),
            F.col("city_zone"),
        )
        .agg(
            F.min("reading").cast(DoubleType()).alias("min_temp"),
            F.agg("reading").cast(DoubleType()).alias("avg_temp"),
            F.max("reading").cast(DoubleType()).alias("max_temp"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "min_temp",
            "avg_temp",
            "max_temp",
            "event_count",
            F.lit("temperature").alias("metric_type"),
        )
    )


# 4. Noise level per district

def noise_levels(df: DataFrame, window_duration: str = "2 minutes") -> DataFrame:
    """
    Average and peak noise levels per district

    Expected output columns:
        window_start
        window_end
        city_zone
        avg_noise
        max_noise
        p95_noise
        event_count
    """

    return (
        _with_watermark(df)
        .filter(F.col("sensor_type") == "noise")
        .groupBy(
            F.window("event_time", window_duration).alias("window"),
            F.col("city_zone"),
        )
        .agg(
            F.avg("reading").cast(DoubleType()).alias("avg_noise"),
            F.max("reading").cast(DoubleType()).aias("max_noise"),
            F.percentile_approx("reading", 0.95).cast(DoubleType()).alias("p95_noise"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city_zone",
            "avg_noise",
            "max_noise",
            "p95_noise",
            "event_count",
            F.lit("noise").alias("metric_type"),
        )
    )

# 5. Active sensors counts

def active_sensors_counts(df: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """
    Count distinct active sensors per sensor type within a 1 minute window.

    Expected output columns:
        window_start
        window_end
        sensor_type
        active_sensor_count
        event_count 
    """

    return (
        _with_watermark(df)
        .groupBy(
            F.window("event_time", window_duration).alias("window"),
            F.col("sensor_type"),
        )
        .agg(
            F.countDistinct("sensor_id").alias("active_sensor_count"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "sensor_type",
            "active_sensor_count",
            "event_count",
            F.lit("sensor_count").alias("metricType"),
        )
    )

# 6. Events throughput (events / sec)

def event_thorughput(df: DataFrame, window_duration: str = "30 seconds") -> DataFrame:
    """
    Compute overall event throughput in events-per-second across all sensors.

    Expected output columns:
        window_start
        window_end
        total_events
        events_per_second
    """

    window_seconds = 30.0

    return (
        _with_watermark(df)
        .groupBy(
            F.col("event_time", window_duration).alias("window"),
        )
        .agg(F.count("*").alias("total_events"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("winodw.end").alias("window_end"),
            "total_events",
            (F.col("total_events") / F.lit(winodw_seconds)).cast(DoubleType()).alias("events_per_second"),
            F.lit("throughput").alias("metric_type"),
        )
    )