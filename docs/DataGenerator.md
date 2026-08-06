# Data Generator

## Purpose

The CineFlux Data Generator creates deterministic synthetic sources for downstream batch and
stream processing. It produces measurable data-quality problems, preserves the configured
batch history, and enforces the source cutover boundary.

| Mode | Output | Time rule |
|---|---|---|
| `bootstrap` | Historical playback events as Parquet in MinIO | `event_timestamp < CUTOVER_TIMESTAMP` |
| `recurring_batch` | User, subscription, and content snapshots as Parquet in MinIO | Configured delivery timestamp |
| `stream` | Playback events as Avro messages in Kafka | `event_timestamp >= CUTOVER_TIMESTAMP` |

Batch and stream generation use separate packages and configuration files. They share only
the seed, entity ranges, and cutover timestamp.

## Generated Data Problems

### Batch

| Problem | Control | Expected behavior |
|---|---|---|
| City skew | `city_distribution` | Ho Chi Minh City represents about 45% of user rows |
| Genre skew | `genre_distribution` | Drama, Action, and Comedy represent about 75% of content rows |
| High cardinality | `user_count`, `content_count`, generated IDs | Large distinct counts for `user_id`, `session_id`, and `event_id` |
| Schema evolution | `schema_evolution` and delivery version | `critic_score` is absent in version 1 and present in version 2 |
| Duplicates | `duplicate_rate` | About 2% duplicate rows before key-based deduplication |

Recurring deliveries are immutable full snapshots. Earlier deliveries remain available so
later processing can derive source history and SCD Type 2 dimensions.

### Stream

| Problem | Control | Expected behavior |
|---|---|---|
| Burst traffic | `burst` | Periodic high-rate intervals |
| Late arrival | `late_arrival_*` | Event time is delayed from production time |
| Out-of-order arrival | `out_of_order_*` | Events are reordered inside bounded buffers |
| Duplicate retry | `duplicate_rate` | About 1.5% of final messages repeat an `event_id` |
| Invalid payload | `invalid_payload_rate` | Malformed messages are tagged for downstream rejection |

Kafka headers carry `execution_id` and simulation flags. These headers do not change the
versioned playback event contract.

## Configuration

The committed YAML files are the complete configuration source:

| Scenario | Shared | Batch | Stream |
|---|---|---|---|
| Smoke | [`shared_smoke.yaml`](../data_platform/generator/config/shared_smoke.yaml) | [`batch_smoke.yaml`](../data_platform/generator/config/batch_smoke.yaml) | [`stream_smoke.yaml`](../data_platform/generator/config/stream_smoke.yaml) |
| Demo | [`shared_demo.yaml`](../data_platform/generator/config/shared_demo.yaml) | [`batch_demo.yaml`](../data_platform/generator/config/batch_demo.yaml) | [`stream_demo.yaml`](../data_platform/generator/config/stream_demo.yaml) |
| Flink benchmark | [`shared_demo.yaml`](../data_platform/generator/config/shared_demo.yaml) | N/A | [`stream_flink_demo.yaml`](../data_platform/generator/config/stream_flink_demo.yaml) |

`event_time_acceleration` advances simulated production time faster than wall-clock time
without changing message counts or problem rates. The smoke stream uses `60.0` for quick
window checks, the paced Flink benchmark uses `5.0`, and the general demo remains at `1.0`.
The Flink benchmark reuses the demo entity ranges and generates 500,000 base events at a
configured base rate of 4,000 events per second.

### Scenario Size

| Setting | Smoke | Demo |
|---|---:|---:|
| Seed | 20260720 | 20260720 |
| Cutover | 2026-01-01 00:00:00 UTC | 2026-01-01 00:00:00 UTC |
| Users | 1,000 | 100,000 |
| Content items | 250 | 25,000 |
| Historical days | 7 | 180 |
| Base bootstrap events | 20,000 | 5,000,000 |
| Parquet rows per file | 5,000 | 25,000 |
| Base stream events | 20,000 | 250,000 |
| Base stream rate | 1,000/s | 2,000/s |
| Stream pacing | Disabled | Enabled |

### Problem Rates

| Setting | Value |
|---|---:|
| Batch duplicates | 2.00% |
| Entity changes per later delivery | 10.00% |
| Version 2 `critic_score` non-null rate | 90.00% |
| Stream duplicates | 1.50% |
| Late arrivals | 5.00% |
| Out-of-order events | 3.00% |
| Invalid payloads | 0.50% |
| Burst schedule | 10-second cycle, 10% duration, 8x rate |

The city and genre weights are declared once in each batch scenario file. Delivery 1 uses
schema version 1 at cutover; delivery 2 uses schema version 2 one day later.

## Storage And Contract

Batch files use Parquet with Snappy compression under the landing bucket:

```text
bootstrap/initial_historical_playback_events/
  scenario=<scenario>/event_date=<yyyy-mm-dd>/part-<number>.parquet

recurring/<dataset>/
  scenario=<scenario>/delivery_date=<yyyy-mm-dd>/
  schema_version=<number>/part-<number>.parquet
```

`<dataset>` is `users`, `subscriptions`, or `content`.

Stream records use the six-partition `playback_events` topic and the versioned contract at
[`schemas/avro/playback_events_v1.avsc`](../schemas/avro/playback_events_v1.avsc). Schema
Registry compatibility is `BACKWARD`. Each Kafka record timestamp equals the event's
`produced_timestamp`, providing a deterministic production-time watermark source.

## Run

Build the two image variants:

```bash
make generator-build-baseline
make generator-build
```

Start only the dependencies needed by each source type, then run the smoke scenario:

```bash
make up PROFILE=generator-batch
make generator-bootstrap GENERATOR_CONFIG=smoke
make generator-batch GENERATOR_CONFIG=smoke DELIVERY=delivery_001
make generator-batch GENERATOR_CONFIG=smoke DELIVERY=delivery_002

make up PROFILE=generator-stream
make generator-stream GENERATOR_CONFIG=smoke EXECUTION_ID=smoke_001
```

Use `GENERATOR_CONFIG=demo` for downstream processing volume. Stream execution IDs should be
unique so messages remain traceable.

Each command prints one JSON summary to standard output. Logs use standard error, so a local
summary can be retained without mixing log lines:

```bash
mkdir -p artifacts/data_generator
make generator-bootstrap GENERATOR_CONFIG=demo \
  > artifacts/data_generator/bootstrap_summary.json
```

The ignored `artifacts/` directory is local only. Summary metrics are calculated during
generation; the generator does not read data back from MinIO or Kafka.

## Evidence Output

Retain the four demo summaries used to document a generation run:

```bash
mkdir -p artifacts/data_generator
make generator-bootstrap GENERATOR_CONFIG=demo \
  > artifacts/data_generator/bootstrap_summary.json
make generator-batch GENERATOR_CONFIG=demo DELIVERY=delivery_001 \
  > artifacts/data_generator/delivery_001_summary.json
make generator-batch GENERATOR_CONFIG=demo DELIVERY=delivery_002 \
  > artifacts/data_generator/delivery_002_summary.json
make generator-stream GENERATOR_CONFIG=demo EXECUTION_ID=demo_001 \
  > artifacts/data_generator/stream_summary.json
```

Together these summaries report city and genre skew, approximate identifier cardinality,
schema-evolution nulls, duplicate counts before and after key deduplication, stream problem
rates, Kafka acknowledgments, cutover violations, and stored Parquet volume.

### Recorded Demo Run

The recorded `demo` run used seed `20260720`, a `2026-01-01T00:00:00Z` cutover,
100,000 users, and 25,000 content items.

| Batch characteristic | Recorded result |
|---|---|
| City skew | Ho Chi Minh City represented 44.9294% and 44.8804% of the two user snapshots; Hanoi represented 20.0833% and 20.1009% |
| Genre skew | Drama represented 29.9373% and 29.8863% of the two content snapshots; Action represented 25.7899% and 25.8291% |
| Approximate cardinality | 99,573 users, 1,675,325 sessions, and 4,941,146 events across bootstrap playback data |
| Schema evolution | `delivery_001` omitted `critic_score`, producing 25,510 nulls after schema merge; `delivery_002` included the field with 2,536 nulls (9.9412%) |
| Batch duplicates | Bootstrap contained 102,060 duplicate rows (2.0004%); each recurring dataset measured 1.9992%, with zero duplicates after key deduplication |
| Cutover enforcement | Zero bootstrap events crossed the cutover boundary |

The bootstrap wrote 5,102,060 rows to 360 Snappy Parquet files totaling 194,716,943
bytes. Each recurring delivery wrote 229,590 rows to nine files. All 378 generated files
were uploaded to the `cineflux-landing` bucket.

| Stream characteristic | Configured | Recorded |
|---|---:|---:|
| Burst duration / multiplier | 10% / 8x | 125,850 burst messages (49.5849% of output) |
| Late arrivals | 5.0% | 12,703 messages (5.0050%) |
| Duplicates | 1.5% | 3,807 messages (1.5000%) |
| Out-of-order events | 3.0% | 7,617 messages (3.0011%) |
| Invalid payloads | 0.5% | 1,250 messages (0.4925%) |

Kafka acknowledged all 253,807 generated messages with zero delivery failures. No stream
event violated the cutover boundary.

## Reproducibility

- Every random decision derives from the configured seed.
- Object names and partition paths are stable.
- Existing objects are compared by SHA-256 metadata: identical reruns are skipped and
  conflicting bytes are rejected.
- Kafka delivery is complete only after all producer callbacks are acknowledged and
  `flush()` returns successfully.
