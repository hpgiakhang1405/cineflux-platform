"""Create required Kafka topics before the Flink source starts.

Reads: Kafka bootstrap servers and versioned topic configuration.
Writes: source and DLQ topics when they do not already exist.
Runs: on the Flink client before stream graph submission.
"""

from __future__ import annotations

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaAdminGateway:
    """Apply idempotent topic creation through Kafka AdminClient."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._client = AdminClient({"bootstrap.servers": bootstrap_servers})

    def ensure_topic(self, topic: str, partitions: int) -> None:
        """Create one single-replica local topic when absent."""
        future = self._client.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
        )[topic]
        try:
            future.result()
        except KafkaException as exc:
            error = exc.args[0]
            if error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
