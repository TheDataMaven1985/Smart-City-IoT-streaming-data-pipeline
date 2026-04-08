"""
Spark Transformations & Schema Definition
"""

import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
from config.settings import VALID_SENSOR_TYPES

# Schema for a raw IoT event as it arrives in Kafka

RAW_EVENT_SCHEMA = StructType(
    [
        StructField("event_id",      StringType(),    nullable=True),
        StructField("sensor_id",     StringType(),    nullable=False),
        StructField("sensor_type",   StringType(),    nullable=False),
        StructField("city_zone",     StringType(),    nullable=True),
        StructField("latitude",      DoubleType(),    nullable=True),
        StructField("longitude",     DoubleType(),    nullable=True),
        StructField("timestamp",     StringType(),    nullable=False),  # ISO-8601
        StructField("reading",       DoubleType(),    nullable=True),
        StructField("battery_level", DoubleType(),    nullable=True),
    ]
)

# Schema with parsed timestamp (after cleaning step)

CLEAN_EVENT_SCHEMA = StructType(
    [
        StructField("event_id",       StringType(),    nullable=True),
        StructField("sensor_id",      StringType(),    nullable=False),
        StructField("sensor_type",    StringType(),    nullable=False),
        StructField("city_zone",      StringType(),    nullable=True),
        StructField("latitude",       DoubleType(),    nullable=True),
        StructField("longitude",      DoubleType(),    nullable=True),
        StructField("event_time",     TimestampType(), nullable=False),
        StructField("reading",        DoubleType(),    nullable=True),
        StructField("battery_level",  DoubleType(),    nullable=True),
        StructField("ingest_time",    TimestampType(), nullable=False),
    ]
)

# Step 1: Parse Kafka binary payload → typed columns

def parse_kafka_payload(raw_df: DataFrame) -> DataFrame:
    """
    Convert the raw kafka DataFrame into typed DataFrame
    using the IoT event schema.
    """

    # cast the binary kafka value UTF-8 string, then parse as JSON
    parsed = raw_df.select(
        F.from_json(
            F.col("value").cast("string"),
            RAW_EVENT_SCHEMA,
        ).alias("data")
    ).select("data.*")

    return parsed


# Step 2: Data cleaning

def clean_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split the stream into valid and invalid DataFrames.
    
    Great Expectations handles deeper checks:
        1. sensor_id  must not be NULL
        2. sensor_type must be one of the predefined categories
        3. timestamp must be parseable as ISO-8601
        4. battery_level must be within [0, 100]
        5. reading must not be NULL (range validation done in GE layer)
    """

    valid_sensor_types = F.array(*[F.lit(t) for t in VALID_SENSOR_TYPES])

    # Parse the ISO-8601 timestamp to a proper TimesstampType
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
    ).withColumn(
        "ingest_time",
        F.current_timestamp(),
    )

    # Build a rejection_reason column (NULL means the record is valid)
    df =  df.withColumn(
        "rejection_reason",
        F.when(F.col("sensor_id").isNull(), F.lit("null_sensor_id"))
        .when(
            ~F.array_contains(valid_sensor_types, F.col("sensor_type")),
            F.lit("invalid_sensor_type"),
        )
        .when(F.col("event_time").isNull(), F.lit("unparseable_timestamp"))
        .when(
            (F.col("battery_level") < 0) | (F.col("battery_level") > 100),
            F.lit("battery_out_of_range")
        )
        .when(F.col("reading").isNull(), F.lit("null_reading"))
        .otherwise(F.lit(None).cast("string")),
    )

    valid_df = (
        df.filter(F.col("rejection_reason").isNull())
        .drop("rejection_reason", "timestamp")  # event_time
    )

    invalid_df = df.filter(F.col("rejection_reason").isNotNull())

    return valid_df, invalid_df


# Step 3: Enrichment — add derived columns

def enrich_events(df: DataFrame) -> DataFrame:
    """
    enchrichment columns to the clean event stream:
        'hour_of_day': 0–23, useful for time-of-day analytics
        'day_of_week': 1 (Sun) – 7 (Sat) per Spark convention
        'reading_zscore': placeholder column (populated by window aggregation
                          in the analytics module)
        'event_date': DATE partition column for Parquet writes
    """

    return (
        df
        .withColumn("hour_of_day", F.hour("event_time"))
        .withColumn("day_of_week", F.dayofweek("event_time"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("year", F.year("event_time").cast("string"))
        .withColumn("month", F.month("event_time").cast("string"), 2, "0")
        .withColumn("day", F.day("event_time").cast("string"), 2, "0")
    )