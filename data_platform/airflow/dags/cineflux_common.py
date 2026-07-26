"""Shared Airflow operator configuration for CineFlux batch pipelines."""

from __future__ import annotations

from datetime import timedelta

from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator

DEFAULT_ARGS = {
    "owner": "cineflux",
    "retries": int(Variable.get("pipeline_retries")),
    "retry_delay": timedelta(
        minutes=int(Variable.get("pipeline_retry_delay_minutes"))
    ),
}

SPARK_ENVIRONMENT = {
    "SPARK_MASTER_URL": "spark://{{ conn.cineflux_spark.host }}:{{ conn.cineflux_spark.port }}",
    "HIVE_METASTORE_URI": "{{ conn.cineflux_hive_metastore.extra_dejson.uri }}",
    "MINIO_ENDPOINT": "{{ conn.cineflux_minio.extra_dejson.endpoint }}",
    "MINIO_ROOT_USER": "{{ conn.cineflux_minio.login }}",
    "MINIO_ROOT_PASSWORD": "{{ conn.cineflux_minio.password }}",
    "MINIO_LANDING_BUCKET": "{{ var.value.minio_landing_bucket }}",
    "MINIO_WAREHOUSE_BUCKET": "{{ var.value.minio_warehouse_bucket }}",
    "CUTOVER_TIMESTAMP": "{{ var.value.cutover_timestamp }}",
    "ICEBERG_CATALOG_NAME": "{{ var.value.iceberg_catalog_name }}",
}

DBT_ENVIRONMENT = {
    "DBT_TARGET": "{{ var.value.dbt_target }}",
    "DBT_TRINO_HOST": "{{ conn.cineflux_trino.host }}",
    "DBT_TRINO_PORT": "{{ conn.cineflux_trino.port }}",
    "DBT_TRINO_USER": "{{ conn.cineflux_trino.login }}",
    "DBT_TRINO_HTTP_SCHEME": "{{ conn.cineflux_trino.extra_dejson.http_scheme }}",
    "DBT_THREADS": "{{ var.value.dbt_threads }}",
    "DBT_ICEBERG_CATALOG": "{{ var.value.iceberg_catalog_name }}",
    "DBT_POSTGRES_CATALOG": "{{ var.value.postgres_catalog_name }}",
    "DBT_SILVER_SCHEMA": "{{ var.value.silver_schema }}",
    "DBT_GOLD_SCHEMA": "{{ var.value.gold_schema }}",
    "DBT_FEATURE_SCHEMA": "{{ var.value.feature_schema }}",
    "POSTGRES_SERVING_SCHEMA": "{{ var.value.serving_schema }}",
}


def spark_task(task_id: str, application_args: list[str]) -> SparkSubmitOperator:
    """Create one Spark task using Airflow-owned runtime configuration."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="cineflux_spark",
        application="{{ var.value.spark_application_path }}",
        py_files="{{ var.value.spark_py_files_path }}",
        jars="{{ var.value.spark_jars }}",
        application_args=application_args,
        deploy_mode="client",
        conf={
            "spark.driver.host": "{{ var.value.spark_driver_host }}",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.memory": "{{ var.value.spark_driver_memory }}",
            "spark.pyspark.driver.python": "{{ var.value.spark_driver_python }}",
            "spark.pyspark.python": "{{ var.value.spark_executor_python }}",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "{{ var.value.spark_event_log_dir }}",
        },
        env_vars=SPARK_ENVIRONMENT,
        status_poll_interval=5,
    )


def dbt_task(task_id: str, command: str, selector_variable: str) -> DockerOperator:
    """Create one dbt task in the existing optimized dbt image."""
    return DockerOperator(
        task_id=task_id,
        image="{{ var.value.dbt_jobs_image }}",
        command=[
            command,
            "--project-dir",
            "{{ var.value.dbt_project_dir }}",
            "--profiles-dir",
            "{{ var.value.dbt_profiles_dir }}",
            "--select",
            f"{{{{ var.value.{selector_variable} }}}}",
        ],
        environment=DBT_ENVIRONMENT,
        docker_url=Variable.get("docker_engine_url"),
        network_mode=Variable.get("docker_network_name"),
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
    )
