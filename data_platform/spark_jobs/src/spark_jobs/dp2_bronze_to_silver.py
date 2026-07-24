"""DP2 normalize Bronze history into deterministic Silver tables.

Reads: four append-only Bronze Iceberg tables.
Writes: four `stg_*` tables and `int_playback_sessions` in Iceberg Silver.
Runs: the `dp2` Spark CLI command and the future Bronze-to-Silver Airflow task.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_jobs.base import SparkJob
from spark_jobs.config import Dp2Config
from spark_jobs.io.iceberg import IcebergTableRepository
from spark_jobs.settings import RuntimeSettings

LOGGER = logging.getLogger(__name__)


class Dp2BronzeToSilverJob(SparkJob):
    """Build clean Silver history and session-level playback data."""

    def __init__(
        self,
        spark: Any,
        run_id: str,
        config: Dp2Config,
        settings: RuntimeSettings,
    ) -> None:
        """Create DP2 with the shared Iceberg repository."""
        super().__init__(spark, run_id)
        self._config = config
        self._settings = settings
        self._iceberg = IcebergTableRepository(spark, settings.catalog_name)

    @property
    def job_name(self) -> str:
        """Return the configured Spark application name."""
        return self._config.job_name

    @property
    def configuration(self) -> dict[str, Any]:
        """Return the non-secret DP2 configuration."""
        return self._config.model_dump(mode="json")

    def read(self) -> dict[str, DataFrame]:
        """Read every Bronze table owned by DP1."""
        bronze = self._config.bronze
        return {
            "users": self._iceberg.read(bronze.namespace, bronze.users),
            "subscriptions": self._iceberg.read(bronze.namespace, bronze.subscriptions),
            "content": self._iceberg.read(bronze.namespace, bronze.content),
            "playback_events": self._iceberg.read(
                bronze.namespace, bronze.playback_events
            ),
        }

    def validate_input(self, inputs: dict[str, DataFrame]) -> None:
        """Reject missing Bronze keys and empty source tables."""
        keys = {
            "users": {"user_id", "source_updated_timestamp"},
            "subscriptions": {"user_id", "source_updated_timestamp"},
            "content": {"content_id", "source_updated_timestamp"},
            "playback_events": {"event_id", "session_id", "event_timestamp"},
        }
        counts: dict[str, int] = {}
        for dataset, frame in inputs.items():
            missing = sorted(keys[dataset].difference(frame.columns))
            if missing:
                raise ValueError(f"{dataset} is missing Bronze columns: {', '.join(missing)}")
            with self.spark_action(
                f"bronze.validate.{dataset}",
                f"Validate Bronze {dataset} row count",
            ):
                count = frame.count()
            if count == 0:
                raise ValueError(f"Bronze table is empty: {dataset}")
            counts[dataset] = count
        self.metrics["input_rows"] = counts

    def transform(self, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Normalize source contracts, deduplicate versions, and aggregate sessions."""
        users = self._stage_users(inputs["users"])
        subscriptions = self._stage_subscriptions(inputs["subscriptions"])
        content = self._stage_content(inputs["content"])
        playback_events = self._stage_playback_events(inputs["playback_events"])
        sessions = self._build_sessions(playback_events, content)
        return {
            "users": users,
            "subscriptions": subscriptions,
            "content": content,
            "playback_events": playback_events,
            "playback_sessions": sessions,
        }

    def write(self, outputs: dict[str, DataFrame]) -> None:
        """Atomically replace Silver tables with deterministic full rebuilds."""
        silver = self._config.silver
        self._iceberg.ensure_namespace(silver.namespace)
        tables = self._table_mapping()
        for dataset, frame in outputs.items():
            table = tables[dataset]
            description = f"Replace Silver {table}"
            if dataset == "playback_events":
                description += f" using {self._config.dedup_strategy} deduplication"
            with self.spark_action(f"silver.write.{table}", description):
                self._iceberg.replace(
                    silver.namespace,
                    table,
                    frame,
                    "event_timestamp" if dataset == "playback_events" else None,
                )

    def validate_output(self, outputs: dict[str, DataFrame]) -> None:
        """Validate uniqueness, retained history, session ranges, and skew results."""
        del outputs
        silver = self._config.silver
        tables = self._table_mapping()
        uniqueness_keys = {
            "users": ["user_id", "source_updated_timestamp"],
            "subscriptions": ["user_id", "source_updated_timestamp"],
            "content": ["content_id", "source_updated_timestamp"],
            "playback_events": ["event_id"],
            "playback_sessions": ["session_id"],
        }
        table_metrics: dict[str, dict[str, int]] = {}
        staged: dict[str, DataFrame] = {}
        for dataset, table in tables.items():
            frame = self._iceberg.read(silver.namespace, table)
            staged[dataset] = frame
            keys = uniqueness_keys[dataset]
            with self.spark_action(
                f"silver.validate.{table}.uniqueness",
                f"Validate Silver {table} uniqueness",
            ):
                row = frame.agg(
                    F.count(F.lit(1)).alias("rows"),
                    F.countDistinct(*keys).alias("unique_keys"),
                ).first()
            rows = int(row["rows"])
            unique_keys = int(row["unique_keys"])
            duplicates = rows - unique_keys
            if duplicates:
                raise ValueError(f"Silver table {table} contains {duplicates} duplicate key(s)")
            table_metrics[dataset] = {
                "rows": rows,
                "unique_business_keys": unique_keys,
                "duplicates": duplicates,
            }

        for dataset, natural_key in (
            ("users", "user_id"),
            ("subscriptions", "user_id"),
            ("content", "content_id"),
        ):
            table = tables[dataset]
            with self.spark_action(
                f"silver.validate.{table}.versions",
                f"Validate Silver {table} retained versions",
            ):
                natural_keys = staged[dataset].select(natural_key).distinct().count()
            table_metrics[dataset]["natural_keys"] = natural_keys
            table_metrics[dataset]["retained_extra_versions"] = (
                table_metrics[dataset]["rows"] - natural_keys
            )

        with self.spark_action(
            "silver.validate.playback_sessions.completion_rate",
            "Validate Silver playback session completion rates",
        ):
            invalid_completion = staged["playback_sessions"].filter(
                (F.col("completion_rate") < F.lit(0.0))
                | (F.col("completion_rate") > F.lit(1.0))
            ).count()
        if invalid_completion:
            raise ValueError(f"Found {invalid_completion} invalid completion rate(s)")

        self.metrics["output_tables"] = table_metrics
        self.metrics["invalid_completion_rates"] = invalid_completion
        self.metrics["skew_distribution"] = self._run_skew_benchmark(
            staged["playback_events"], staged["users"], staged["content"]
        )

    def publish_lineage(self) -> None:
        """Log Bronze-to-Silver lineage for later metadata integration."""
        bronze = self._config.bronze
        silver = self._config.silver
        LOGGER.info(
            "spark_lineage run_id=%s source=%s.%s targets=%s.%s",
            self.run_id,
            self._settings.catalog_name,
            bronze.namespace,
            self._settings.catalog_name,
            silver.namespace,
        )

    def _stage_users(self, frame: DataFrame) -> DataFrame:
        """Deduplicate and normalize user profile versions."""
        selected = self._deduplicate(frame, ["user_id", "source_updated_timestamp"])
        return selected.select(
            F.col("user_id"),
            F.upper(F.trim(F.col("country_code"))).alias("country_code"),
            F.initcap(F.lower(F.trim(F.col("city")))).alias("city"),
            F.col("birth_year").cast("int"),
            F.lower(F.trim(F.col("preferred_language"))).alias("preferred_language"),
            F.col("marketing_opt_in").cast("boolean").alias("is_marketing_opt_in"),
            F.col("source_updated_timestamp").cast("timestamp"),
            F.col("_ingested_at").alias("ingested_timestamp"),
            F.col("_record_hash").alias("record_hash"),
            F.lit(self.run_id).alias("pipeline_run_id"),
        )

    def _stage_subscriptions(self, frame: DataFrame) -> DataFrame:
        """Deduplicate and validate subscription versions."""
        accepted = frame.filter(
            F.col("subscription_tier").isin("basic", "standard", "premium")
            & F.col("subscription_status").isin("active", "paused", "cancelled")
        )
        selected = self._deduplicate(
            accepted, ["user_id", "source_updated_timestamp"]
        )
        return selected.select(
            F.col("user_id"),
            F.lower(F.trim(F.col("subscription_tier"))).alias("subscription_tier"),
            F.lower(F.trim(F.col("subscription_status"))).alias("subscription_status"),
            F.upper(F.trim(F.col("billing_country_code"))).alias(
                "billing_country_code"
            ),
            F.col("started_date").cast("date"),
            F.col("ended_date").cast("date"),
            F.col("source_updated_timestamp").cast("timestamp"),
            F.col("_ingested_at").alias("ingested_timestamp"),
            F.col("_record_hash").alias("record_hash"),
            F.lit(self.run_id).alias("pipeline_run_id"),
        )

    def _stage_content(self, frame: DataFrame) -> DataFrame:
        """Merge the evolved content contract and retain all distinct versions."""
        if "critic_score" not in frame.columns:
            frame = frame.withColumn("critic_score", F.lit(None).cast("double"))
        accepted = frame.filter(
            F.col("content_type").isin("movie", "series", "documentary")
            & F.col("availability_status").isin("available", "unavailable")
            & (F.col("runtime_minutes") > F.lit(0))
        )
        selected = self._deduplicate(
            accepted, ["content_id", "source_updated_timestamp"]
        )
        return selected.select(
            F.col("content_id"),
            F.lower(F.trim(F.col("content_type"))).alias("content_type"),
            F.trim(F.col("title")).alias("title"),
            F.transform(F.col("genres"), lambda value: F.initcap(F.trim(value))).alias(
                "genres"
            ),
            F.col("release_year").cast("int"),
            F.col("runtime_minutes").cast("int"),
            F.upper(F.trim(F.col("maturity_rating"))).alias("maturity_rating"),
            F.lower(F.trim(F.col("original_language"))).alias("original_language"),
            F.lower(F.trim(F.col("availability_status"))).alias(
                "availability_status"
            ),
            F.col("critic_score").cast("double"),
            F.col("source_updated_timestamp").cast("timestamp"),
            F.col("_ingested_at").alias("ingested_timestamp"),
            F.col("_record_hash").alias("record_hash"),
            F.lit(self.run_id).alias("pipeline_run_id"),
        )

    def _stage_playback_events(self, frame: DataFrame) -> DataFrame:
        """Validate the batch playback contract and deduplicate event IDs."""
        accepted = frame.filter(
            F.col("event_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("content_id").isNotNull()
            & F.col("session_id").isNotNull()
            & F.col("event_type").isin(
                "playback_started", "playback_progressed", "playback_completed"
            )
            & (F.col("position_seconds") >= F.lit(0))
            & (F.col("watch_seconds") >= F.lit(0))
            & (F.col("event_timestamp") < F.lit(self._settings.cutover_timestamp))
        )
        selected = self._deduplicate(accepted, ["event_id"])
        return selected.select(
            F.col("event_id"),
            F.col("user_id"),
            F.col("content_id"),
            F.col("session_id"),
            F.lower(F.trim(F.col("event_type"))).alias("event_type"),
            F.col("position_seconds").cast("long"),
            F.col("watch_seconds").cast("long"),
            F.col("event_timestamp").cast("timestamp"),
            F.col("produced_timestamp").cast("timestamp"),
            F.col("payload_schema_version").cast("int"),
            F.col("_ingested_at").alias("ingested_timestamp"),
            F.col("_record_hash").alias("record_hash"),
            F.lit(self.run_id).alias("pipeline_run_id"),
        )

    def _build_sessions(self, events: DataFrame, content: DataFrame) -> DataFrame:
        """Aggregate exact session grain while controlling high-cardinality shuffle size."""
        current_content_window = Window.partitionBy("content_id").orderBy(
            F.col("source_updated_timestamp").desc(),
            F.col("ingested_timestamp").desc(),
            F.col("record_hash").desc(),
        )
        current_content = (
            content.withColumn("_rank", F.row_number().over(current_content_window))
            .filter(F.col("_rank") == F.lit(1))
            .select(
                "content_id",
                (F.col("runtime_minutes") * F.lit(60)).cast("long").alias(
                    "content_runtime_seconds"
                ),
            )
        )
        rollup = (
            events.repartition(self._config.session_shuffle_partitions, "session_id")
            .groupBy("session_id", "user_id", "content_id")
            .agg(
                F.min("event_timestamp").alias("session_started_timestamp"),
                F.max("event_timestamp").alias("session_ended_timestamp"),
                F.max("watch_seconds").cast("long").alias("watch_seconds"),
                F.max(
                    F.when(F.col("event_type") == "playback_completed", F.lit(1)).otherwise(
                        F.lit(0)
                    )
                ).alias("has_completed_event"),
                F.count(F.lit(1)).cast("long").alias("event_count"),
            )
        )
        enriched = rollup.join(F.broadcast(current_content), "content_id", "left")
        completion_rate = F.when(
            F.col("content_runtime_seconds") > F.lit(0),
            F.least(
                F.lit(1.0),
                F.col("watch_seconds").cast("double")
                / F.col("content_runtime_seconds").cast("double"),
            ),
        ).otherwise(F.lit(0.0))
        return enriched.select(
            F.col("session_id"),
            F.col("user_id"),
            F.col("content_id"),
            F.col("session_started_timestamp"),
            F.col("session_ended_timestamp"),
            F.col("watch_seconds"),
            F.col("content_runtime_seconds"),
            completion_rate.alias("completion_rate"),
            (
                (F.col("has_completed_event") == F.lit(1))
                | (completion_rate >= F.lit(self._config.completion_threshold))
            ).alias("is_completed"),
            F.col("event_count"),
            F.lit(self.started_at).alias("created_timestamp"),
            F.lit(self.run_id).alias("pipeline_run_id"),
        )

    def _deduplicate(self, frame: DataFrame, keys: list[str]) -> DataFrame:
        """Select one deterministic row for each Silver business key."""
        ordering = [
            F.col("_schema_version").desc(),
            F.col("_ingested_at").desc(),
            F.col("_ingestion_id").desc(),
            F.col("_record_hash").desc(),
        ]
        window = Window.partitionBy(*keys).orderBy(*ordering)
        return (
            frame.withColumn("_dedup_rank", F.row_number().over(window))
            .filter(F.col("_dedup_rank") == F.lit(1))
            .drop("_dedup_rank")
        )

    def _run_skew_benchmark(
        self, events: DataFrame, users: DataFrame, content: DataFrame
    ) -> dict[str, list[dict[str, Any]]]:
        """Run city and genre distributions with direct or salted aggregation."""
        user_window = Window.partitionBy("user_id").orderBy(
            F.col("source_updated_timestamp").desc(), F.col("ingested_timestamp").desc()
        )
        content_window = Window.partitionBy("content_id").orderBy(
            F.col("source_updated_timestamp").desc(), F.col("ingested_timestamp").desc()
        )
        current_users = (
            users.withColumn("_rank", F.row_number().over(user_window))
            .filter(F.col("_rank") == F.lit(1))
            .select("user_id", "city")
        )
        current_content = (
            content.withColumn("_rank", F.row_number().over(content_window))
            .filter(F.col("_rank") == F.lit(1))
            .select("content_id", F.element_at("genres", 1).alias("primary_genre"))
        )
        enriched = (
            events.select("event_id", "user_id", "content_id")
            .join(F.broadcast(current_users), "user_id", "left")
            .join(F.broadcast(current_content), "content_id", "left")
            .persist(StorageLevel.DISK_ONLY)
        )
        try:
            return {
                "city": self._aggregate_skew(enriched, "city"),
                "genre": self._aggregate_skew(enriched, "primary_genre"),
            }
        finally:
            enriched.unpersist()

    def _aggregate_skew(self, frame: DataFrame, value_column: str) -> list[dict[str, Any]]:
        """Aggregate one skewed category using the configured benchmark strategy."""
        selected = frame.select("event_id", value_column)
        grouping_columns = [value_column]
        if self._config.skew_strategy == "salted":
            selected = selected.withColumn(
                "_salt",
                F.pmod(F.xxhash64("event_id"), F.lit(self._config.skew_salt_buckets)),
            )
            grouping_columns.append("_salt")

        # Materializing the raw repartition makes hot-key imbalance visible in Spark UI.
        partitioned = selected.repartition(
            self._config.spark.shuffle_partitions, *grouping_columns
        ).persist(StorageLevel.DISK_ONLY)
        try:
            description = (
                f"Benchmark {self._config.skew_strategy} "
                f"{value_column} skew distribution"
            )
            with self.spark_action(
                f"benchmark.skew.{value_column}.{self._config.skew_strategy}",
                description,
            ):
                partitioned.count()
                if self._config.skew_strategy == "salted":
                    counts = (
                        partitioned.groupBy(value_column, "_salt")
                        .count()
                        .groupBy(value_column)
                        .agg(F.sum("count").alias("count"))
                    )
                else:
                    counts = partitioned.groupBy(value_column).count()
                total = counts.agg(F.sum("count").alias("total")).first()["total"]
                return [
                    {
                        "value": row[value_column],
                        "count": int(row["count"]),
                        "share": round(float(row["count"]) / float(total), 6),
                    }
                    for row in counts.orderBy(
                        F.col("count").desc(), F.col(value_column)
                    ).collect()
                ]
        finally:
            partitioned.unpersist()

    def _table_mapping(self) -> dict[str, str]:
        """Map transformation names to configured Silver table names."""
        tables = self._config.silver
        return {
            "users": tables.users,
            "subscriptions": tables.subscriptions,
            "content": tables.content,
            "playback_events": tables.playback_events,
            "playback_sessions": tables.playback_sessions,
        }
