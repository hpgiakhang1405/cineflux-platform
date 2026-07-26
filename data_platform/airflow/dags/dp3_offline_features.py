"""Orchestrate offline feature computation and dbt validation."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from cineflux_common import DEFAULT_ARGS, dbt_task, trino_table

GOLD_INPUTS = [
    trino_table("gold_schema", "obt_user_content_engagement"),
    trino_table("gold_schema", "mart_content_trending"),
]
FEATURE_OUTPUTS = [
    trino_table("feature_schema", "feat_user_engagement"),
    trino_table("feature_schema", "feat_content_popularity"),
]

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
            inlets=GOLD_INPUTS,
            outlets=FEATURE_OUTPUTS,
        )

    with TaskGroup(group_id="validate") as validate:
        validate_offline_features = dbt_task(
            "feature_contract_tests",
            "test",
            "dbt_dp3_selector",
            inlets=FEATURE_OUTPUTS,
        )

    ingest >> validate
