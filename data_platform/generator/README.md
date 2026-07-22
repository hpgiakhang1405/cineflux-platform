# CineFlux Data Generator

## Purpose

This component produces deterministic synthetic source data:

- Batch commands write historical playback and recurring master-data snapshots as Parquet
  files in MinIO.
- The stream command publishes Avro playback events to Kafka through Schema Registry.

Configured problems include skew, high-cardinality IDs, schema evolution, duplicates,
bursts, late arrivals, out-of-order events, and invalid payloads.

## Configuration

Each scenario has shared, batch, and stream YAML files under `config/`. The shared file
defines the seed, cutover timestamp, and entity ranges. Batch and stream files independently
define their volumes and problem rates.

Use `smoke` for fast checks and `demo` for downstream processing volume.

## Build And Run

Local development uses the Python version pinned in `.python-version`. Create the locked
runtime environment with:

```bash
uv sync --frozen --no-dev
```

Run from the repository root:

```bash
make generator-build-baseline
make generator-build

make up PROFILE=generator-batch
make generator-bootstrap GENERATOR_CONFIG=smoke
make generator-batch GENERATOR_CONFIG=smoke DELIVERY=delivery_001
make generator-batch GENERATOR_CONFIG=smoke DELIVERY=delivery_002

make up PROFILE=generator-stream
make generator-stream GENERATOR_CONFIG=smoke EXECUTION_ID=smoke_001
```

Required environment variables are listed in the root `.env.example`.

## Inputs And Outputs

| Command | Input | Output |
|---|---|---|
| `bootstrap` | Shared and batch YAML | Historical playback Parquet files |
| `recurring_batch` | Shared and batch YAML plus delivery ID | User, subscription, and content Parquet files |
| `stream` | Shared and stream YAML plus execution ID | Avro playback events in Kafka |

Each command prints a JSON quality summary to standard output after external delivery is
complete. Logs use standard error. Summaries are calculated during generation and do not
read data back from MinIO or Kafka.

See [`docs/DataGenerator.md`](../../docs/DataGenerator.md) for data characteristics,
configuration values, commands, and recorded evidence.
