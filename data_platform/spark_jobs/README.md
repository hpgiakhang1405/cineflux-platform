# Spark Processing Jobs

This component owns CineFlux batch processing from landing files to Iceberg Bronze and
from Bronze to Silver.

| Command | Input | Output |
|---|---|---|
| `dp1` | Generator Parquet files in the MinIO landing bucket | Four append-only Iceberg `raw_*` tables |
| `dp2` | Four Iceberg Bronze tables | Four `stg_*` tables and `int_playback_sessions` |
| `compact` | Small Iceberg files in `bronze.raw_playback_events` | Bin-packed files with unchanged logical rows |

Runtime endpoints and credentials are read from environment variables. Job behavior and
benchmark strategies are selected through versioned YAML files in `config/`.

## Run

The canonical runtime is Docker Compose so the driver, workers, Iceberg dependencies, and
Python environment remain identical across runs:

```bash
make spark-build
make spark-up
make spark-dp1 SPARK_CONFIG=spark_dp1_smoke PIPELINE_RUN_ID=smoke_dp1_001
make spark-dp2 SPARK_CONFIG=spark_dp2_smoke PIPELINE_RUN_ID=smoke_dp2_001
make storage-prepare PIPELINE_RUN_ID=storage_ingestion_001
make storage-compact PIPELINE_RUN_ID=storage_compaction_001
```

The storage ingestion configuration writes playback events in deterministic source batches
inside each daily partition. The compaction command later rewrites those files without
changing the row count or event keys. `storage-prepare` replaces the four Bronze tables and
must only be used when resetting the storage benchmark baseline.

Use the named baseline and optimized files in `config/` for reproducible performance runs.
Each command prints a JSON summary containing its application ID, duration, effective
configuration, row counts, and validation metrics.

| Configuration | Scope |
|---|---|
| `spark_dp1_baseline.yaml` / `spark_dp1_optimized.yaml` | Schema merge versus explicit landing schemas |
| `spark_dp2_baseline.yaml` | Untuned Bronze-to-Silver reference |
| `spark_dp2_skew_optimized.yaml` | Isolated salted skew handling |
| `spark_dp2_cardinality_optimized.yaml` | Isolated session partitioning |
| `spark_dp2_duplicates_optimized.yaml` | Isolated zero-spill deduplication tuning |
| `spark_dp2_optimized.yaml` | Combined production candidate |

Recorded Spark UI evidence, benchmark results, correctness checks, and Trino validation are
documented in [`docs/ProcessingJobs.md`](../../docs/ProcessingJobs.md).

For local package inspection without submitting a job:

```bash
cd data_platform/spark_jobs
uv sync --frozen
uv run cineflux-spark-jobs --help
```

## Runtime Configuration

Shared endpoints, credentials, bucket names, catalog settings, and the batch/stream cutover
are supplied through the root `.env` file. The required variable names are documented in
the root `.env.example`; secrets are never stored in this component.

`Dockerfile.baseline` keeps the naive single-runtime-stage dependency installation for
image-size comparison. `Dockerfile` is the optimized runtime used by every processing and
performance run.
