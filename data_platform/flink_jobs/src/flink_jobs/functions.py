"""Implement stateful stream validation, late handling, and deduplication.

Reads: raw Kafka rows and validated playback rows.
Writes: accepted playback rows, DLQ bytes, and Flink custom metrics.
Runs: as named Python operators inside the Flink execution graph.
"""

from __future__ import annotations

from typing import Any

from pyflink.common import Row, Types
from pyflink.datastream import OutputTag, RuntimeContext
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

from flink_jobs.contracts import ConfluentAvroCodec, build_dlq_values
from flink_jobs.models import ContractViolation

PLAYBACK_FIELD_NAMES = [
    "event_id",
    "user_id",
    "content_id",
    "session_id",
    "event_type",
    "position_seconds",
    "watch_seconds",
    "event_timestamp",
    "produced_timestamp",
    "payload_schema_version",
]
PLAYBACK_TYPE = Types.ROW_NAMED(
    PLAYBACK_FIELD_NAMES,
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.INT(),
        Types.INT(),
        Types.LONG(),
        Types.LONG(),
        Types.INT(),
    ],
)
DLQ_TYPE = Types.ROW_NAMED(
    ["dlq_key", "payload"],
    [Types.PRIMITIVE_ARRAY(Types.BYTE()), Types.PRIMITIVE_ARRAY(Types.BYTE())],
)


class ContractValidationFunction(ProcessFunction):
    """Validate Confluent Avro records and route failures to a side output."""

    def __init__(
        self,
        playback_codec: ConfluentAvroCodec,
        dlq_codec: ConfluentAvroCodec,
        source_topic: str,
        dlq_tag: OutputTag,
    ) -> None:
        self._playback_codec = playback_codec
        self._dlq_codec = dlq_codec
        self._source_topic = source_topic
        self._dlq_tag = dlq_tag
        self._invalid_counter: Any = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Register the invalid-record counter in the Flink metric group."""
        self._invalid_counter = runtime_context.get_metrics_group().counter(
            "invalid_events_routed"
        )

    def process_element(self, value: Row, ctx: ProcessFunction.Context):
        """Yield one accepted event or one encoded DLQ envelope."""
        kafka_key, payload, partition, offset, source_timestamp = value
        try:
            event = self._playback_codec.decode_playback(bytes(payload))
        except Exception as exc:
            violation = (
                exc
                if isinstance(exc, ContractViolation)
                else ContractViolation("unexpected_validation_error", str(exc)[:1000])
            )
            self._invalid_counter.inc()
            dlq_values = build_dlq_values(
                source_topic=self._source_topic,
                source_partition=partition,
                source_offset=offset,
                source_timestamp=(
                    source_timestamp.to_epoch_milli()
                    if source_timestamp is not None
                    else None
                ),
                original_key=bytes(kafka_key) if kafka_key is not None else None,
                original_payload=bytes(payload),
                violation=violation,
            )
            encoded = self._dlq_codec.encode(dlq_values)
            yield self._dlq_tag, Row(dlq_values["dlq_id"].encode(), encoded)
            return
        yield Row(*(getattr(event, field_name) for field_name in PLAYBACK_FIELD_NAMES))


class LateAndDuplicateFunction(KeyedProcessFunction):
    """Count/drop out-of-watermark events and deduplicate event IDs with state."""

    def __init__(
        self, *, late_handling_enabled: bool, dedup_enabled: bool, dedup_ttl_minutes: int
    ) -> None:
        self._late_handling_enabled = late_handling_enabled
        self._dedup_enabled = dedup_enabled
        self._ttl_ms = dedup_ttl_minutes * 60 * 1000
        self._seen_state: Any = None
        self._late_counter: Any = None
        self._duplicate_counter: Any = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Create checkpointed keyed state and observable counters."""
        self._late_counter = runtime_context.get_metrics_group().counter(
            "late_events_dropped"
        )
        self._duplicate_counter = runtime_context.get_metrics_group().counter(
            "duplicate_events_dropped"
        )
        if self._dedup_enabled:
            descriptor = ValueStateDescriptor("event_id_seen", Types.BOOLEAN())
            self._seen_state = runtime_context.get_state(descriptor)

    def process_element(self, value: Row, ctx: KeyedProcessFunction.Context):
        """Apply the configured late and duplicate policies before windowing."""
        if self._late_handling_enabled and value[7] <= ctx.timer_service().current_watermark():
            self._late_counter.inc()
            return
        if self._dedup_enabled:
            if self._seen_state.value():
                self._duplicate_counter.inc()
                return
            self._seen_state.update(True)
            cleanup_time = ctx.timer_service().current_processing_time() + self._ttl_ms
            ctx.timer_service().register_processing_time_timer(cleanup_time)
        yield value

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        """Release deduplication state after the configured processing-time TTL."""
        if self._dedup_enabled:
            self._seen_state.clear()
