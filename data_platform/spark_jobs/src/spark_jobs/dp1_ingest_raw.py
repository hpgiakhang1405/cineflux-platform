"""DP1 ingest immutable MinIO landing files into append-only Bronze.

Reads: generator bootstrap and recurring Parquet objects in MinIO.
Writes: four `raw_*` Iceberg tables with shared technical metadata.
Runs: the `dp1` Spark CLI command and the Airflow raw-to-Bronze task.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark_jobs.base import SparkJob
from spark_jobs.config import Dp1Config
from spark_jobs.io.iceberg import IcebergTableRepository
from spark_jobs.io.landing import LandingRepository
from spark_jobs.schemas import (
    CONTENT_SCHEMA,
    PLAYBACK_EVENT_SCHEMA,
    SUBSCRIPTION_SCHEMA,
    USER_SCHEMA,
)
from spark_jobs.settings import RuntimeSettings

LOGGER = logging.getLogger(__name__)


class Dp1IngestRawJob(SparkJob):
    """Ingest one configured generator scenario into append-only Bronze tables."""

    def __init__(
        self,
        spark: Any,
        run_id: str,
        config: Dp1Config,
        settings: RuntimeSettings,
    ) -> None:
        """Create DP1 with explicit landing and Iceberg gateways."""
        super().__init__(spark, run_id)
        self._config = config
        self._settings = settings
        self._landing = LandingRepository(spark, settings.landing_bucket)
        self._iceberg = IcebergTableRepository(spark, settings.catalog_name)
        self._input_counts: dict[str, int] = {}

    @property
    def job_name(self) -> str:
        """Return the configured Spark application name."""
        return self._config.job_name

    @property
    def configuration(self) -> dict[str, Any]:
        """Return the non-secret DP1 configuration."""
        return self._config.model_dump(mode="json")

    def read(self) -> dict[str, DataFrame]:
        """Read the four source datasets for one landing scenario."""
        landing = self._config.landing
        recurring_root = landing.recurring_prefix.strip("/")
        scenario = landing.scenario
        content_schema = CONTENT_SCHEMA if self._config.schema_strategy == "explicit" else None
        merge_content = self._config.schema_strategy == "merge"
        return {
            "users": self._landing.read(
                f"{recurring_root}/users/scenario={scenario}", USER_SCHEMA, False
            ),
            "subscriptions": self._landing.read(
                f"{recurring_root}/subscriptions/scenario={scenario}",
                SUBSCRIPTION_SCHEMA,
                False,
            ),
            "content": self._landing.read(
                f"{recurring_root}/content/scenario={scenario}",
                content_schema,
                merge_content,
            ),
            "playback_events": self._landing.read(
                f"{landing.bootstrap_prefix.strip('/')}/scenario={scenario}",
                PLAYBACK_EVENT_SCHEMA,
                False,
            ),
        }

    def validate_input(self, inputs: dict[str, DataFrame]) -> None:
        """Validate required identifiers and enforce the bootstrap cutover."""
        required = {
            "users": {"user_id", "source_updated_timestamp"},
            "subscriptions": {"user_id", "source_updated_timestamp"},
            "content": {"content_id", "source_updated_timestamp"},
            "playback_events": {"event_id", "event_timestamp"},
        }
        for dataset, frame in inputs.items():
            missing = sorted(required[dataset].difference(frame.columns))
            if missing:
                raise ValueError(f"{dataset} is missing required columns: {', '.join(missing)}")
            with self.spark_action(
                f"landing.validate.{dataset}",
                f"Validate landing {dataset} row count",
            ):
                count = frame.count()
            if count == 0:
                raise ValueError(f"Landing dataset is empty: {dataset}")
            self._input_counts[dataset] = count

        with self.spark_action(
            "landing.validate.playback_cutover",
            "Validate bootstrap playback cutover boundary",
        ):
            cutover_violations = inputs["playback_events"].filter(
                F.col("event_timestamp") >= F.lit(self._settings.cutover_timestamp)
            ).count()
        if cutover_violations:
            raise ValueError(
                f"Bootstrap contains {cutover_violations} event(s) at or after cutover"
            )
        self.metrics["input_rows"] = self._input_counts
        self.metrics["cutover_violations"] = cutover_violations

    def transform(self, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Attach deterministic technical metadata without removing source duplicates."""
        return {
            dataset: self._with_bronze_metadata(frame, dataset)
            for dataset, frame in inputs.items()
        }

    def write(self, outputs: dict[str, DataFrame]) -> None:
        """Append each source dataset to its owned Bronze Iceberg table."""
        namespace = self._config.bronze.namespace
        tables = self._table_mapping()
        self._iceberg.ensure_namespace(namespace)

        existing_counts: dict[str, int] = {}
        for dataset, table in tables.items():
            if not self._iceberg.exists(namespace, table):
                existing_counts[dataset] = 0
                continue
            with self.spark_action(
                f"bronze.idempotency.{table}",
                f"Check Bronze {table} idempotency",
            ):
                existing_counts[dataset] = (
                    self._iceberg.read(namespace, table)
                    .filter(F.col("_pipeline_run_id") == self.run_id)
                    .count()
                )

        if existing_counts == self._input_counts:
            self.metrics["idempotent_reuse"] = True
            LOGGER.info(
                "bronze_run_reused run_id=%s namespace=%s",
                self.run_id,
                namespace,
            )
            return
        if any(existing_counts.values()):
            raise ValueError(
                f"Pipeline run has partial Bronze output in {namespace}: "
                f"run_id={self.run_id} counts={existing_counts}"
            )

        self.metrics["idempotent_reuse"] = False

        for dataset, frame in outputs.items():
            table = tables[dataset]
            with self.spark_action(
                f"bronze.write.{table}",
                f"Append {dataset} to Bronze {table}",
            ):
                if (
                    dataset == "playback_events"
                    and self._config.playback_write_batches > 1
                ):
                    self._append_playback_batches(namespace, table, frame)
                    continue
                self._iceberg.append(
                    namespace,
                    table,
                    frame,
                    "event_timestamp" if dataset == "playback_events" else None,
                )

    def validate_output(self, outputs: dict[str, DataFrame]) -> None:
        """Confirm every input row was committed under this pipeline run ID."""
        del outputs
        namespace = self._config.bronze.namespace
        output_counts: dict[str, int] = {}
        for dataset, table in self._table_mapping().items():
            with self.spark_action(
                f"bronze.validate.{table}",
                f"Validate Bronze {table} output rows",
            ):
                count = (
                    self._iceberg.read(namespace, table)
                    .filter(F.col("_pipeline_run_id") == self.run_id)
                    .count()
                )
            if count != self._input_counts[dataset]:
                raise ValueError(
                    f"Bronze row count mismatch for {table}: input={self._input_counts[dataset]} output={count}"
                )
            output_counts[dataset] = count

        content = self._iceberg.read(namespace, self._config.bronze.content).filter(
            F.col("_pipeline_run_id") == self.run_id
        )
        with self.spark_action(
            "bronze.validate.content_schema_evolution",
            "Summarize Bronze content schema evolution",
        ):
            evolution_rows = [
                {
                    "schema_version": int(row["_schema_version"]),
                    "rows": int(row["rows"]),
                    "critic_score_nulls": int(row["critic_score_nulls"]),
                }
                for row in (
                    content.groupBy("_schema_version")
                    .agg(
                        F.count(F.lit(1)).alias("rows"),
                        F.sum(F.col("critic_score").isNull().cast("long")).alias(
                            "critic_score_nulls"
                        ),
                    )
                    .orderBy("_schema_version")
                    .collect()
                )
            ]
        self.metrics["output_rows"] = output_counts
        self.metrics["content_schema_evolution"] = evolution_rows

    def publish_lineage(self) -> None:
        """Log DP1 source-to-target lineage for later metadata integration."""
        namespace = self._config.bronze.namespace
        targets = [
            self._iceberg.qualified(namespace, table)
            for table in self._table_mapping().values()
        ]
        LOGGER.info(
            "spark_lineage run_id=%s source=s3a://%s targets=%s",
            self.run_id,
            self._settings.landing_bucket,
            ",".join(targets),
        )

    def _with_bronze_metadata(self, frame: DataFrame, dataset: str) -> DataFrame:
        """Add shared Bronze metadata while preserving the physical source fields."""
        source_columns = sorted(frame.columns)
        source_file = F.input_file_name()
        schema_version: Column
        if dataset == "playback_events":
            schema_version = F.col("payload_schema_version")
        else:
            schema_version = F.regexp_extract(source_file, r"schema_version=(\d+)", 1).cast(
                "int"
            )
        record_hash = F.sha2(
            F.to_json(F.struct(*[F.col(column).alias(column) for column in source_columns])),
            256,
        )
        return (
            frame.withColumn("_source_file", source_file)
            .withColumn(
                "_ingestion_id",
                F.sha2(F.concat_ws("||", F.lit(self.run_id), source_file), 256),
            )
            .withColumn("_ingested_at", F.lit(self.started_at))
            .withColumn("_source_system", F.lit(self._config.source_system))
            .withColumn("_source_object", F.lit(dataset))
            .withColumn("_source_partition", F.lit(None).cast("int"))
            .withColumn("_source_offset", F.lit(None).cast("long"))
            .withColumn("_schema_version", schema_version)
            .withColumn("_record_hash", record_hash)
            .withColumn("_pipeline_run_id", F.lit(self.run_id))
        )

    def _table_mapping(self) -> dict[str, str]:
        """Map source dataset names to configured Bronze table names."""
        tables = self._config.bronze
        return {
            "users": tables.users,
            "subscriptions": tables.subscriptions,
            "content": tables.content,
            "playback_events": tables.playback_events,
        }

    def _append_playback_batches(
        self,
        namespace: str,
        table: str,
        frame: DataFrame,
    ) -> None:
        """Write deterministic source batches that expose the small-file problem."""
        batch_count = self._config.playback_write_batches
        bucketed = frame.withColumn(
            "_write_batch",
            F.pmod(F.xxhash64("event_id"), F.lit(batch_count)),
        )
        for batch_number in range(batch_count):
            batch = bucketed.filter(
                F.col("_write_batch") == F.lit(batch_number)
            ).drop("_write_batch")
            self._iceberg.append(
                namespace,
                table,
                batch,
                "event_timestamp",
            )
