#!/usr/bin/env bash
# Apply the idempotent PostgreSQL streaming-table migration on a fresh volume.

set -euo pipefail

psql \
    --username "${POSTGRES_USER}" \
    --dbname "${CINEFLUX_POSTGRES_DB}" \
    --set=streaming_schema="${POSTGRES_STREAMING_SCHEMA}" \
    --set=ON_ERROR_STOP=1 \
    --file=/platform-migrations/001_streaming_tables.sql
