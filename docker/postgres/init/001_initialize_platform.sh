#!/usr/bin/env bash
# Create the platform databases and PostgreSQL serving schemas.

set -euo pipefail

create_database() {
    local database_name="$1"

    if ! psql --username "${POSTGRES_USER}" --dbname postgres --tuples-only --no-align \
        --command "SELECT 1 FROM pg_database WHERE datname = '${database_name}'" | grep -q 1; then
        createdb --username "${POSTGRES_USER}" "${database_name}"
    fi
}

for database_name in \
    "${CINEFLUX_POSTGRES_DB}" \
    "${HIVE_METASTORE_DB}" \
    "${AIRFLOW_DB}" \
    "${SUPERSET_DB}" \
    "${DATAHUB_DB}"; do
    create_database "${database_name}"
done

psql \
    --username "${POSTGRES_USER}" \
    --dbname "${CINEFLUX_POSTGRES_DB}" \
    --set=serving_schema="${POSTGRES_SERVING_SCHEMA}" \
    --set=streaming_schema="${POSTGRES_STREAMING_SCHEMA}" \
    --set=ON_ERROR_STOP=1 <<'SQL'
SELECT format('CREATE SCHEMA IF NOT EXISTS %I', :'serving_schema') \gexec
SELECT format('CREATE SCHEMA IF NOT EXISTS %I', :'streaming_schema') \gexec
SQL
