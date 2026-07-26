"""Orchestrate offline feature computation and dbt validation."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from cineflux_common import DEFAULT_ARGS, dbt_task

with DAG(
    dag_id="dp3_offline_features",
    description="Compute offline feature tables from Gold and validate feature contracts.",
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cineflux", "batch", "dp3"],
) as dag:
    with TaskGroup(group_id="ingest") as ingest:
        compute_offline_features = dbt_task(
            "compute_offline_features",
            "run",
            "dbt_dp3_selector",
        )

    with TaskGroup(group_id="validate") as validate:
        validate_offline_features = dbt_task(
            "feature_contract_tests",
            "test",
            "dbt_dp3_selector",
        )

    ingest >> validate
