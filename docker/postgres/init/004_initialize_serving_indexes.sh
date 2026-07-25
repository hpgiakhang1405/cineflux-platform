#!/usr/bin/env bash
# Apply the idempotent PostgreSQL serving-index migration on a fresh volume.

set -euo pipefail

psql \
    --username "${POSTGRES_USER}" \
    --dbname "${CINEFLUX_POSTGRES_DB}" \
    --set=serving_schema="${POSTGRES_SERVING_SCHEMA}" \
    --set=ON_ERROR_STOP=1 \
    --file=/platform-migrations/003_serving_indexes.sql
