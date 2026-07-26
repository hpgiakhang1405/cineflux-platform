"""Orchestrate Bronze-to-Silver, Gold, and PostgreSQL serving publication."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from cineflux_common import DEFAULT_ARGS, dbt_task, postgres_table, spark_task, trino_table

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

BRONZE_INPUTS = [
    trino_table("bronze_schema", "raw_users"),
    trino_table("bronze_schema", "raw_subscriptions"),
    trino_table("bronze_schema", "raw_content"),
    trino_table("bronze_schema", "raw_playback_events"),
]
SILVER_OUTPUTS = [
    trino_table("silver_schema", "stg_users"),
    trino_table("silver_schema", "stg_subscriptions"),
    trino_table("silver_schema", "stg_content"),
    trino_table("silver_schema", "stg_playback_events"),
    trino_table("silver_schema", "int_playback_sessions"),
]
GOLD_OUTPUTS = [
    trino_table("gold_schema", "dim_user"),
    trino_table("gold_schema", "dim_content"),
    trino_table("gold_schema", "fact_playback_session"),
    trino_table("gold_schema", "obt_user_content_engagement"),
    trino_table("gold_schema", "mart_content_trending"),
    postgres_table("serving_schema", "mart_content_trending"),
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
        bronze_to_silver = spark_task(
            "bronze_to_silver",
            DP2_ARGS,
            inlets=BRONZE_INPUTS,
            outlets=SILVER_OUTPUTS,
        )
        gold_and_serving = dbt_task(
            "gold_and_serving",
            "run",
            "dbt_dp2_selector",
            inlets=SILVER_OUTPUTS,
            outlets=GOLD_OUTPUTS,
        )
        bronze_to_silver >> gold_and_serving

    with TaskGroup(group_id="validate") as validate:
        validate_silver = spark_task(
            "silver_quality",
            [*DP2_ARGS, "--validate-only"],
            inlets=SILVER_OUTPUTS,
        )
        validate_gold_and_serving = dbt_task(
            "gold_and_serving_tests",
            "test",
            "dbt_dp2_selector",
            inlets=GOLD_OUTPUTS,
        )
        validate_silver >> validate_gold_and_serving

    ingest >> validate
