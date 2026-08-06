"""Decode and encode Confluent-wire Avro records without external I/O.

Reads: raw Kafka value bytes and parsed Avro schemas.
Writes: validated playback models or encoded DLQ envelope bytes.
Runs: inside Python Flink operators and local validation checks.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from io import BytesIO
from typing import Any

from fastavro import parse_schema, schemaless_reader, schemaless_writer
from pydantic import ValidationError

from flink_jobs.models import ContractViolation, PlaybackEvent


class ConfluentAvroCodec:
    """Validate one expected schema ID and encode/decode its wire payloads."""

    def __init__(self, schema: dict[str, Any], schema_id: int) -> None:
        self._schema = parse_schema(schema)
        self._schema_id = schema_id

    def decode_playback(self, payload: bytes) -> PlaybackEvent:
        """Decode a playback payload and enforce the complete domain contract."""
        values = self._decode(payload)
        for field_name in ("event_timestamp", "produced_timestamp"):
            value = values.get(field_name)
            if isinstance(value, datetime):
                values[field_name] = round(value.timestamp() * 1000)
        try:
            return PlaybackEvent.model_validate(values)
        except ValidationError as exc:
            raise ContractViolation("contract_validation_failed", str(exc)[:1000]) from exc

    def encode(self, values: dict[str, Any]) -> bytes:
        """Encode one mapping with Confluent magic byte and schema ID."""
        buffer = BytesIO()
        buffer.write(b"\x00")
        buffer.write(self._schema_id.to_bytes(4, byteorder="big", signed=False))
        schemaless_writer(buffer, self._schema, values)
        return buffer.getvalue()

    def _decode(self, payload: bytes) -> dict[str, Any]:
        """Decode raw bytes after validating Confluent framing and schema identity."""
        if len(payload) < 5 or payload[0] != 0:
            raise ContractViolation(
                "invalid_confluent_wire_format",
                "Payload does not contain a Confluent magic byte and schema ID",
            )
        schema_id = int.from_bytes(payload[1:5], byteorder="big", signed=False)
        if schema_id != self._schema_id:
            raise ContractViolation(
                "unexpected_schema_id",
                f"Expected schema ID {self._schema_id}, received {schema_id}",
            )
        buffer = BytesIO(payload[5:])
        try:
            values = schemaless_reader(buffer, self._schema)
        except Exception as exc:
            raise ContractViolation("avro_decode_failed", str(exc)[:1000]) from exc
        if buffer.tell() != len(payload) - 5:
            raise ContractViolation("trailing_payload_bytes", "Payload contains trailing bytes")
        if not isinstance(values, dict):
            raise ContractViolation("invalid_avro_record", "Decoded Avro value is not a record")
        return values


def build_dlq_values(
    *,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    source_timestamp: int | datetime | None,
    original_key: bytes | None,
    original_payload: bytes,
    violation: ContractViolation,
) -> dict[str, Any]:
    """Create a deterministic replay-ready DLQ envelope."""
    identity = f"{source_topic}:{source_partition}:{source_offset}".encode()
    return {
        "dlq_id": hashlib.sha256(identity).hexdigest(),
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "source_timestamp": source_timestamp,
        "original_key": original_key,
        "original_payload": original_payload,
        "error_type": violation.code,
        "error_message": str(violation),
        "detected_timestamp": datetime.now(UTC),
        "replay_count": 0,
    }
