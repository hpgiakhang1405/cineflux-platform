#!/usr/bin/env bash
# Bootstrap the Airflow metadata database, user, connections, and variables.

set -euo pipefail

upsert_connection() {
    local connection_id="$1"
    shift
    airflow connections delete "${connection_id}" >/dev/null 2>&1 || true
    airflow connections add "${connection_id}" "$@"
}

set_variable() {
    airflow variables set "$1" "$2"
}

airflow db migrate

if ! airflow users list | grep -q "${AIRFLOW_ADMIN_USERNAME}"; then
    airflow users create \
        --username "${AIRFLOW_ADMIN_USERNAME}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}" \
        --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
        --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
        --role Admin \
        --email "${AIRFLOW_ADMIN_EMAIL}"
fi

spark_endpoint="${SPARK_MASTER_URL#spark://}"
spark_host="${spark_endpoint%:*}"
spark_port="${SPARK_MASTER_URL##*:}"
minio_endpoint="${MINIO_ENDPOINT#*://}"
minio_host="${minio_endpoint%:*}"
minio_port="${MINIO_ENDPOINT##*:}"
kafka_host="${KAFKA_BOOTSTRAP_SERVERS%:*}"
kafka_port="${KAFKA_BOOTSTRAP_SERVERS##*:}"
postgres_host="${POSTGRES_ENDPOINT%:*}"
postgres_port="${POSTGRES_ENDPOINT##*:}"
hive_metastore_endpoint="${HIVE_METASTORE_URI#*://}"
hive_metastore_host="${hive_metastore_endpoint%:*}"
hive_metastore_port="${HIVE_METASTORE_URI##*:}"

upsert_connection cineflux_spark \
    --conn-type spark \
    --conn-host "${spark_host}" \
    --conn-port "${spark_port}" \
    --conn-description "CineFlux Spark standalone cluster"
upsert_connection cineflux_minio \
    --conn-type generic \
    --conn-host "${minio_host}" \
    --conn-login "${MINIO_ROOT_USER}" \
    --conn-password "${MINIO_ROOT_PASSWORD}" \
    --conn-port "${minio_port}" \
    --conn-extra "{\"endpoint\":\"${MINIO_ENDPOINT}\"}" \
    --conn-description "CineFlux MinIO object storage"
upsert_connection cineflux_kafka \
    --conn-type generic \
    --conn-host "${kafka_host}" \
    --conn-port "${kafka_port}" \
    --conn-description "CineFlux Kafka broker"
upsert_connection cineflux_trino \
    --conn-type generic \
    --conn-host "${DBT_TRINO_HOST}" \
    --conn-login "${DBT_TRINO_USER}" \
    --conn-port "${DBT_TRINO_PORT}" \
    --conn-extra "{\"http_scheme\":\"${DBT_TRINO_HTTP_SCHEME}\"}" \
    --conn-description "CineFlux Trino query engine"
upsert_connection cineflux_postgres \
    --conn-type postgres \
    --conn-host "${postgres_host}" \
    --conn-login "${POSTGRES_USER}" \
    --conn-password "${POSTGRES_PASSWORD}" \
    --conn-port "${postgres_port}" \
    --conn-schema "${CINEFLUX_POSTGRES_DB}" \
    --conn-description "CineFlux PostgreSQL serving database"
upsert_connection cineflux_hive_metastore \
    --conn-type generic \
    --conn-host "${hive_metastore_host}" \
    --conn-port "${hive_metastore_port}" \
    --conn-extra "{\"uri\":\"${HIVE_METASTORE_URI}\"}" \
    --conn-description "CineFlux Hive Metastore"

set_variable cutover_timestamp "${CUTOVER_TIMESTAMP}"
set_variable minio_landing_bucket "${MINIO_LANDING_BUCKET}"
set_variable minio_warehouse_bucket "${MINIO_WAREHOUSE_BUCKET}"
set_variable iceberg_catalog_name "${ICEBERG_CATALOG_NAME}"
set_variable postgres_catalog_name "${DBT_POSTGRES_CATALOG}"
set_variable postgres_database "${CINEFLUX_POSTGRES_DB}"
set_variable bronze_schema "${AIRFLOW_BRONZE_SCHEMA}"
set_variable silver_schema "${DBT_SILVER_SCHEMA}"
set_variable gold_schema "${DBT_GOLD_SCHEMA}"
set_variable feature_schema "${DBT_FEATURE_SCHEMA}"
set_variable serving_schema "${POSTGRES_SERVING_SCHEMA}"
set_variable landing_scenario "${AIRFLOW_LANDING_SCENARIO}"
set_variable landing_bootstrap_prefix "${AIRFLOW_LANDING_BOOTSTRAP_PREFIX}"
set_variable landing_recurring_prefix "${AIRFLOW_LANDING_RECURRING_PREFIX}"
set_variable spark_application_path "${AIRFLOW_SPARK_APPLICATION_PATH}"
set_variable spark_py_files_path "${AIRFLOW_SPARK_PY_FILES_PATH}"
set_variable spark_dp1_config_path "${AIRFLOW_SPARK_DP1_CONFIG_PATH}"
set_variable spark_dp2_config_path "${AIRFLOW_SPARK_DP2_CONFIG_PATH}"
set_variable spark_compaction_config_path "${AIRFLOW_SPARK_COMPACTION_CONFIG_PATH}"
set_variable spark_snapshot_expiration_config_path "${AIRFLOW_SPARK_SNAPSHOT_EXPIRATION_CONFIG_PATH}"
set_variable spark_jars "${AIRFLOW_SPARK_JARS}"
set_variable spark_driver_host "${AIRFLOW_SPARK_DRIVER_HOST}"
set_variable spark_driver_memory "${SPARK_DRIVER_MEMORY}"
set_variable spark_driver_python "${AIRFLOW_SPARK_DRIVER_PYTHON}"
set_variable spark_executor_python "${AIRFLOW_SPARK_EXECUTOR_PYTHON}"
set_variable spark_event_log_dir "${AIRFLOW_SPARK_EVENT_LOG_DIR}"
set_variable lineage_trino_namespace "${DATAHUB_TRINO_SCHEME}://${DBT_TRINO_HOST}:${DBT_TRINO_PORT}"
set_variable lineage_postgres_namespace "postgres://${POSTGRES_ENDPOINT}"
set_variable lineage_landing_namespace "s3://${MINIO_LANDING_BUCKET}"
set_variable lineage_trino_platform_instance "${DATAHUB_TRINO_PLATFORM_INSTANCE}"
set_variable lineage_postgres_platform_instance "${DATAHUB_POSTGRES_PLATFORM_INSTANCE}"
set_variable dbt_jobs_image "${DBT_JOBS_IMAGE}"
set_variable dbt_project_dir "${AIRFLOW_DBT_PROJECT_DIR}"
set_variable dbt_profiles_dir "${AIRFLOW_DBT_PROFILES_DIR}"
set_variable dbt_target "${DBT_TARGET}"
set_variable dbt_threads "${DBT_THREADS}"
set_variable dbt_dp2_selector "${AIRFLOW_DBT_DP2_SELECTOR}"
set_variable dbt_dp3_selector "${AIRFLOW_DBT_DP3_SELECTOR}"
set_variable docker_engine_url "${DOCKER_ENGINE_URL}"
set_variable docker_network_name "${DOCKER_NETWORK_NAME}"
set_variable pipeline_retries "${AIRFLOW_PIPELINE_RETRIES}"
set_variable pipeline_retry_delay_minutes "${AIRFLOW_PIPELINE_RETRY_DELAY_MINUTES}"
set_variable iceberg_maintenance_schedule "${AIRFLOW_ICEBERG_MAINTENANCE_SCHEDULE}"
