"""Publish Avro playback events and intentionally malformed payloads to Kafka.

Reads: Kafka and Schema Registry endpoints plus a versioned Avro schema file.
Writes: valid Confluent-wire-format messages and explicitly tagged malformed bytes.
Runs: stream generation and its automatic post-publish summary.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.request import Request, urlopen

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from generator.io.protocols import DeliveryResult
from generator.stream.models import StreamEnvelope, StreamEvent


class KafkaEventPublisher:
    """Create the topic, register the contract, and publish stream envelopes."""

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        partitions: int,
        schema_path: Path,
    ) -> None:
        """Initialize Kafka administration, serialization, and producer clients."""
        self._bootstrap_servers = bootstrap_servers
        self._schema_registry_url = schema_registry_url.rstrip("/")
        self._topic = topic
        self._partitions = partitions
        self._schema_text = schema_path.read_text(encoding="utf-8")
        self._schema_registry = SchemaRegistryClient({"url": self._schema_registry_url})
        self._serializer = AvroSerializer(self._schema_registry, self._schema_text)
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "snappy",
                "linger.ms": 20,
            }
        )
        self._acknowledged_messages = 0
        self._delivery_errors: list[str] = []

    def prepare(self, probe: StreamEvent) -> None:
        """Create the topic and register the Avro subject before the first publish."""
        admin = AdminClient({"bootstrap.servers": self._bootstrap_servers})
        futures = admin.create_topics(
            [NewTopic(self._topic, num_partitions=self._partitions, replication_factor=1)]
        )
        try:
            futures[self._topic].result()
        except KafkaException as exc:
            error = exc.args[0]
            if error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        context = SerializationContext(self._topic, MessageField.VALUE)
        self._serializer(probe.model_dump(mode="python"), context)
        request = Request(
            f"{self._schema_registry_url}/config/{self._topic}-value",
            data=json.dumps({"compatibility": "BACKWARD"}).encode(),
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            method="PUT",
        )
        with urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise RuntimeError("Unable to set Schema Registry compatibility")

    def publish(self, envelope: StreamEnvelope, execution_id: str) -> None:
        """Publish one valid or intentionally malformed Kafka message."""
        headers: list[tuple[str, str | bytes | None]] = [
            ("execution_id", execution_id.encode()),
            ("is_burst", _flag(envelope.is_burst)),
            ("is_late", _flag(envelope.is_late)),
            ("is_out_of_order", _flag(envelope.is_out_of_order)),
            ("is_duplicate", _flag(envelope.is_duplicate)),
            ("is_invalid", _flag(envelope.is_invalid)),
        ]
        if envelope.is_invalid:
            payload = f'{{"event_id":"{envelope.event.event_id}","malformed":'.encode()
        else:
            payload = self._serializer(
                envelope.event.model_dump(mode="python"),
                SerializationContext(self._topic, MessageField.VALUE),
            )
        self._producer.produce(
            self._topic,
            key=envelope.event.session_id.encode(),
            value=payload,
            headers=headers,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> DeliveryResult:
        """Block until all queued messages receive Kafka delivery callbacks."""
        remaining = self._producer.flush(30)
        if remaining:
            raise RuntimeError(f"Kafka producer still has {remaining} undelivered messages")
        if self._delivery_errors:
            raise RuntimeError(
                f"Kafka delivery failed for {len(self._delivery_errors)} messages: "
                f"{self._delivery_errors[0]}"
            )
        return DeliveryResult(self._acknowledged_messages, 0)

    def _on_delivery(self, error: KafkaError | None, _: Message) -> None:
        """Record one broker acknowledgment or delivery failure."""
        if error is None:
            self._acknowledged_messages += 1
        else:
            self._delivery_errors.append(str(error))


def _flag(value: bool) -> bytes:
    """Encode one boolean simulation flag as a compact Kafka header."""
    return b"1" if value else b"0"
