"""Orchestrate Bronze-to-Silver, Gold, and PostgreSQL serving publication."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from cineflux_common import DEFAULT_ARGS, dbt_task, spark_task

DP2_ARGS = [
    "dp2",
    "--config",
    "{{ var.value.spark_dp2_config_path }}",
    "--run-id",
    "{{ run_id }}",
    "--namespace",
    "{{ var.value.bronze_schema }}",
    "--silver-namespace",
    "{{ var.value.silver_schema }}",
]

with DAG(
    dag_id="dp2_bronze_to_gold",
    description="Build Silver and Gold, publish serving data, then validate both layers.",
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cineflux", "batch", "dp2"],
) as dag:
    with TaskGroup(group_id="ingest") as ingest:
        bronze_to_silver = spark_task("bronze_to_silver", DP2_ARGS)
        gold_and_serving = dbt_task(
            "gold_and_serving",
            "run",
            "dbt_dp2_selector",
        )
        bronze_to_silver >> gold_and_serving

    with TaskGroup(group_id="validate") as validate:
        validate_silver = spark_task(
            "silver_quality",
            [*DP2_ARGS, "--validate-only"],
        )
        validate_gold_and_serving = dbt_task(
            "gold_and_serving_tests",
            "test",
            "dbt_dp2_selector",
        )
        validate_silver >> validate_gold_and_serving

    ingest >> validate
