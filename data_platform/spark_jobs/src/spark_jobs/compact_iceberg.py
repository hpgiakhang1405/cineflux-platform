"""Compact small Iceberg data files without changing table contents.

Reads: one configured Iceberg table and its current data-file metadata.
Writes: bin-packed data files through Iceberg's rewrite procedure.
Runs: the `compact` Spark CLI command for repeatable storage maintenance.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.base import SparkJob
from spark_jobs.config import IcebergCompactionConfig
from spark_jobs.io.iceberg import IcebergTableRepository
from spark_jobs.settings import RuntimeSettings

LOGGER = logging.getLogger(__name__)


class IcebergCompactionJob(SparkJob):
    """Rewrite small data files and verify that logical table data is unchanged."""

    def __init__(
        self,
        spark: Any,
        run_id: str,
        config: IcebergCompactionConfig,
        settings: RuntimeSettings,
    ) -> None:
        """Create the compaction job for one configured Iceberg table."""
        super().__init__(spark, run_id)
        self._config = config
        self._settings = settings
        self._iceberg = IcebergTableRepository(spark, settings.catalog_name)
        self._before_data: dict[str, Any] = {}

    @property
    def job_name(self) -> str:
        """Return the configured Spark application name."""
        return self._config.job_name

    @property
    def configuration(self) -> dict[str, Any]:
        """Return the non-secret compaction configuration."""
        return self._config.model_dump(mode="json")

    def read(self) -> dict[str, DataFrame]:
        """Read the configured target table."""
        table = self._config.table
        if not self._iceberg.exists(table.namespace, table.name):
            raise ValueError(
                f"Iceberg table does not exist: {table.namespace}.{table.name}"
            )
        return {"target": self._iceberg.read(table.namespace, table.name)}

    def validate_input(self, inputs: dict[str, DataFrame]) -> None:
        """Capture exact table and physical-file metrics before rewriting files."""
        target = inputs["target"]
        required = {
            self._config.table.business_key,
            self._config.table.event_timestamp_column,
        }
        missing = sorted(required.difference(target.columns))
        if missing:
            raise ValueError(
                f"Compaction target is missing columns: {', '.join(missing)}"
            )
        self._before_data = self._data_metrics(target)
        self.metrics["before"] = {
            "data": self._before_data,
            "files": self._file_metrics(),
        }

    def transform(self, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Return the target unchanged because compaction is a physical rewrite."""
        return inputs

    def write(self, outputs: dict[str, DataFrame]) -> None:
        """Run Iceberg bin-pack compaction with the configured target file size."""
        del outputs
        table = self._config.table
        target = f"{table.namespace}.{table.name}"
        statement = f"""
            CALL {self._settings.catalog_name}.system.rewrite_data_files(
                table => '{target}',
                strategy => '{self._config.strategy}',
                options => map(
                    'target-file-size-bytes', '{self._config.target_file_size_bytes}',
                    'min-input-files', '{self._config.min_input_files}',
                    'partial-progress.enabled', 'false'
                )
            )
        """
        with self.spark_action(
            "iceberg.compaction.rewrite_data_files",
            f"Compact Iceberg {target}",
        ):
            result = self.spark.sql(statement).first()
        self.metrics["rewrite"] = result.asDict(recursive=True) if result else {}

    def validate_output(self, outputs: dict[str, DataFrame]) -> None:
        """Verify unchanged logical data and fewer, larger physical files."""
        del outputs
        table = self._config.table
        target = self._iceberg.read(table.namespace, table.name)
        after_data = self._data_metrics(target)
        after_files = self._file_metrics()
        self.metrics["after"] = {"data": after_data, "files": after_files}
        if after_data != self._before_data:
            raise ValueError("Compaction changed the logical table metrics")
        before_file_count = self.metrics["before"]["files"]["file_count"]
        if (
            self._config.require_file_reduction
            and after_files["file_count"] >= before_file_count
        ):
            raise ValueError(
                "Compaction did not reduce the number of current data files"
            )

    def publish_lineage(self) -> None:
        """Log the maintained table as both the source and target."""
        table = self._config.table
        target = self._iceberg.qualified(table.namespace, table.name)
        LOGGER.info(
            "spark_maintenance run_id=%s operation=rewrite_data_files target=%s",
            self.run_id,
            target,
        )

    def _data_metrics(self, frame: DataFrame) -> dict[str, Any]:
        """Return exact content metrics used to prove logical equivalence."""
        table = self._config.table
        row = frame.agg(
            F.count(F.lit(1)).alias("row_count"),
            F.countDistinct(table.business_key).alias("business_key_count"),
            F.min(table.event_timestamp_column).alias("minimum_event_timestamp"),
            F.max(table.event_timestamp_column).alias("maximum_event_timestamp"),
        ).first()
        return {
            "row_count": int(row["row_count"]),
            "business_key_count": int(row["business_key_count"]),
            "minimum_event_timestamp": row[
                "minimum_event_timestamp"
            ].isoformat(),
            "maximum_event_timestamp": row[
                "maximum_event_timestamp"
            ].isoformat(),
        }

    def _file_metrics(self) -> dict[str, int]:
        """Return current Iceberg data-file count and size metrics."""
        table = self._config.table
        qualified = self._iceberg.qualified(table.namespace, table.name)
        metadata = self.spark.table(
            f"{qualified}.files"
        ).filter(F.col("content") == F.lit(0))
        row = metadata.agg(
            F.count(F.lit(1)).alias("file_count"),
            F.sum("file_size_in_bytes").alias("total_file_size_bytes"),
            F.avg("file_size_in_bytes").alias("average_file_size_bytes"),
        ).first()
        partition_row = metadata.groupBy("partition").count().agg(
            F.count(F.lit(1)).alias("partition_count"),
            F.min("count").alias("minimum_files_per_partition"),
            F.max("count").alias("maximum_files_per_partition"),
        ).first()
        snapshot = (
            self.spark.table(f"{qualified}.snapshots")
            .orderBy(F.col("committed_at").desc())
            .select("snapshot_id")
            .first()
        )
        return {
            "snapshot_id": int(snapshot["snapshot_id"]),
            "file_count": int(row["file_count"]),
            "total_file_size_bytes": int(row["total_file_size_bytes"]),
            "average_file_size_bytes": round(
                float(row["average_file_size_bytes"])
            ),
            "partition_count": int(partition_row["partition_count"]),
            "minimum_files_per_partition": int(
                partition_row["minimum_files_per_partition"]
            ),
            "maximum_files_per_partition": int(
                partition_row["maximum_files_per_partition"]
            ),
        }
