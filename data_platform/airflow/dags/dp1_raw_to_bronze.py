"""Orchestrate landing-to-Bronze ingestion and independent validation."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from cineflux_common import DEFAULT_ARGS, spark_task

DP1_ARGS = [
    "dp1",
    "--config",
    "{{ var.value.spark_dp1_config_path }}",
    "--run-id",
    "{{ run_id }}",
    "--landing-scenario",
    "{{ var.value.landing_scenario }}",
    "--bootstrap-prefix",
    "{{ var.value.landing_bootstrap_prefix }}",
    "--recurring-prefix",
    "{{ var.value.landing_recurring_prefix }}",
    "--namespace",
    "{{ var.value.bronze_schema }}",
]

with DAG(
    dag_id="dp1_raw_to_bronze",
    description="Ingest immutable landing files into Bronze and validate the persisted run.",
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cineflux", "batch", "dp1"],
) as dag:
    with TaskGroup(group_id="ingest") as ingest:
        ingest_raw_to_bronze = spark_task("raw_to_bronze", DP1_ARGS)

    with TaskGroup(group_id="validate") as validate:
        validate_bronze = spark_task(
            "bronze_contract_and_counts",
            [*DP1_ARGS, "--validate-only"],
        )

    ingest >> validate
