"""Expire old Iceberg snapshots while retaining a safe rollback window."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.base import SparkJob
from spark_jobs.config import IcebergSnapshotExpirationConfig
from spark_jobs.io.iceberg import IcebergTableRepository
from spark_jobs.settings import RuntimeSettings

LOGGER = logging.getLogger(__name__)


class IcebergSnapshotExpirationJob(SparkJob):
    """Remove snapshots older than retention without removing recent rollback points."""

    def __init__(
        self,
        spark: Any,
        run_id: str,
        config: IcebergSnapshotExpirationConfig,
        settings: RuntimeSettings,
    ) -> None:
        super().__init__(spark, run_id)
        self._config = config
        self._settings = settings
        self._iceberg = IcebergTableRepository(spark, settings.catalog_name)
        self._before_snapshot_count = 0
        self._latest_snapshot_id = 0

    @property
    def job_name(self) -> str:
        return self._config.job_name

    @property
    def configuration(self) -> dict[str, Any]:
        return self._config.model_dump(mode="json")

    def read(self) -> dict[str, DataFrame]:
        table = self._config.table
        if not self._iceberg.exists(table.namespace, table.name):
            raise ValueError(
                f"Iceberg table does not exist: {table.namespace}.{table.name}"
            )
        snapshots = self.spark.table(
            f"{self._iceberg.qualified(table.namespace, table.name)}.snapshots"
        )
        return {"snapshots": snapshots}

    def validate_input(self, inputs: dict[str, DataFrame]) -> None:
        row = inputs["snapshots"].agg(
            F.count(F.lit(1)).alias("snapshot_count"),
            F.max_by("snapshot_id", "committed_at").alias("latest_snapshot_id"),
        ).first()
        self._before_snapshot_count = int(row["snapshot_count"])
        if self._before_snapshot_count == 0:
            raise ValueError("Snapshot expiration target has no snapshots")
        self._latest_snapshot_id = int(row["latest_snapshot_id"])
        self.metrics["before_snapshot_count"] = self._before_snapshot_count

    def transform(self, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        return inputs

    def write(self, outputs: dict[str, DataFrame]) -> None:
        del outputs
        table = self._config.table
        target = f"{table.namespace}.{table.name}"
        older_than = self.started_at - timedelta(hours=self._config.retention_hours)
        timestamp = older_than.strftime("%Y-%m-%d %H:%M:%S.%f")
        statement = f"""
            CALL {self._settings.catalog_name}.system.expire_snapshots(
                table => '{target}',
                older_than => TIMESTAMP '{timestamp}',
                retain_last => {self._config.retain_last}
            )
        """
        with self.spark_action(
            "iceberg.maintenance.expire_snapshots",
            f"Expire old Iceberg snapshots for {target}",
        ):
            result = self.spark.sql(statement).first()
        self.metrics["expiration_result"] = (
            result.asDict(recursive=True) if result else {}
        )
        self.metrics["older_than"] = older_than.isoformat()

    def validate_output(self, outputs: dict[str, DataFrame]) -> None:
        del outputs
        table = self._config.table
        snapshots = self.spark.table(
            f"{self._iceberg.qualified(table.namespace, table.name)}.snapshots"
        )
        row = snapshots.agg(
            F.count(F.lit(1)).alias("snapshot_count"),
            F.max_by("snapshot_id", "committed_at").alias("latest_snapshot_id"),
        ).first()
        after_count = int(row["snapshot_count"])
        if after_count > self._before_snapshot_count:
            raise ValueError("Snapshot expiration increased the snapshot count")
        if int(row["latest_snapshot_id"]) != self._latest_snapshot_id:
            raise ValueError("Snapshot expiration removed the latest snapshot")
        self.metrics["after_snapshot_count"] = after_count

    def publish_lineage(self) -> None:
        table = self._config.table
        LOGGER.info(
            "spark_maintenance run_id=%s operation=expire_snapshots target=%s",
            self.run_id,
            self._iceberg.qualified(table.namespace, table.name),
        )
