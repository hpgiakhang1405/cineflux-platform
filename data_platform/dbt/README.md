# dbt Jobs

This component uses dbt Core and dbt-trino to transform Iceberg Silver tables into Gold dimensional models, analytical marts, and offline feature tables. It also publishes selected marts to PostgreSQL through Trino.

## Run

All commands run through the repository Makefile and use the root `.env` file:

```bash
make dbt-build
make dbt-up
make dbt-debug
make dbt-smoke
make dbt-run
make dbt-test
make dbt-docs
make dbt-docs-serve
```

`make dbt-run` applies the idempotent PostgreSQL serving-table migration before executing
the models. dbt docs are available at `http://localhost:8085` while the docs server runs.

## Inputs And Outputs

- Reads: `iceberg.silver.*`
- Writes: `iceberg.gold.*`, `iceberg.feature.*`
- Publishes: `postgresql.serving.mart_content_trending`
