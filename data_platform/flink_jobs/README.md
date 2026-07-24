# Flink Jobs

This component validates Confluent-wire Avro playback events from Kafka, applies event-time
watermarks and stateful deduplication, routes invalid records to a replay-ready DLQ, and
upserts five-minute aggregates into PostgreSQL `streaming.*`.

## Inputs And Outputs

| Direction | Resource |
|---|---|
| Input | Kafka `playback_events` with `playback_events-value` Avro contract |
| Output | PostgreSQL `streaming.playback_metrics_5m` |
| Output | PostgreSQL `streaming.content_popularity_5m` |
| Error output | Kafka `playback_events_dlq` with its own Avro contract |
| State | Embedded RocksDB with checkpoints stored in MinIO |

## Run

From the repository root:

```bash
make flink-build
make flink-up
make flink-migrate
make flink-submit FLINK_CONFIG=flink_smoke
```

The Flink dashboard is available at `http://localhost:8083`. Runtime endpoints and
credentials come from the repository `.env`; benchmark behavior comes from files under
`config/`.

Cancel a completed experiment and reset its Kafka and PostgreSQL data before the next run:

```bash
make flink-cancel FLINK_JOB_ID=<job-id>
make flink-reset-data FLINK_CONFIG=flink_smoke
```

The reset command refuses to run while a Flink job is active.
