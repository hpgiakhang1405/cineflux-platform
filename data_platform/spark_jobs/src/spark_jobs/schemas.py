"""Declare explicit source schemas for landing Parquet files.

Reads: no external state.
Writes: reusable Spark StructType contracts.
Runs: DP1 explicit-schema ingestion and source validation.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


USER_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("city", StringType(), False),
        StructField("birth_year", LongType(), False),
        StructField("preferred_language", StringType(), False),
        StructField("marketing_opt_in", BooleanType(), False),
        StructField("source_updated_timestamp", TimestampType(), False),
    ]
)

SUBSCRIPTION_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("subscription_tier", StringType(), False),
        StructField("subscription_status", StringType(), False),
        StructField("billing_country_code", StringType(), False),
        StructField("started_date", DateType(), False),
        StructField("ended_date", DateType(), True),
        StructField("source_updated_timestamp", TimestampType(), False),
    ]
)

CONTENT_SCHEMA = StructType(
    [
        StructField("content_id", StringType(), False),
        StructField("content_type", StringType(), False),
        StructField("title", StringType(), False),
        StructField("genres", ArrayType(StringType(), containsNull=False), False),
        StructField("release_year", LongType(), False),
        StructField("runtime_minutes", LongType(), False),
        StructField("maturity_rating", StringType(), False),
        StructField("original_language", StringType(), False),
        StructField("availability_status", StringType(), False),
        StructField("source_updated_timestamp", TimestampType(), False),
        StructField("critic_score", DoubleType(), True),
    ]
)

PLAYBACK_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("content_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("position_seconds", LongType(), False),
        StructField("watch_seconds", LongType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("produced_timestamp", TimestampType(), False),
        StructField("payload_schema_version", LongType(), False),
    ]
)
