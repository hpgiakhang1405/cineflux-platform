"""Schedule safe Iceberg compaction and snapshot expiration."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from cineflux_common import DEFAULT_ARGS, spark_task

with DAG(
    dag_id="iceberg_maintenance",
    description="Compact small files and expire old snapshots with rollback retention.",
    start_date=datetime(2026, 7, 20),
    schedule=Variable.get("iceberg_maintenance_schedule"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cineflux", "iceberg", "maintenance"],
) as dag:
    compact_small_files = spark_task(
        "compact_small_files",
        [
            "compact",
            "--config",
            "{{ var.value.spark_compaction_config_path }}",
            "--run-id",
            "{{ run_id }}",
            "--namespace",
            "{{ var.value.bronze_schema }}",
        ],
    )
    expire_old_snapshots = spark_task(
        "expire_old_snapshots",
        [
            "expire-snapshots",
            "--config",
            "{{ var.value.spark_snapshot_expiration_config_path }}",
            "--run-id",
            "{{ run_id }}",
            "--namespace",
            "{{ var.value.bronze_schema }}",
        ],
    )

    compact_small_files >> expire_old_snapshots
