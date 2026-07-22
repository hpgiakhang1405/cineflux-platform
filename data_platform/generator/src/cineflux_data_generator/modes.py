"""Compose deployable generator modes through a single Factory.

Reads: command selection, validated configuration, and runtime environment settings.
Writes: mode runners for bootstrap, recurring batch, or Kafka stream execution.
Runs: the command-line entrypoint.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from cineflux_data_generator.batch.generator import BatchDataGenerator
from cineflux_data_generator.batch.runner import BatchRunner
from cineflux_data_generator.config import load_batch_config, load_stream_config
from cineflux_data_generator.io.kafka_publisher import KafkaEventPublisher
from cineflux_data_generator.io.minio_writer import MinioObjectWriter
from cineflux_data_generator.settings import BatchRuntimeSettings, StreamRuntimeSettings
from cineflux_data_generator.stream.generator import StreamDataGenerator
from cineflux_data_generator.stream.runner import StreamRunner
from cineflux_data_generator.stream.summary import build_stream_summary


class GeneratorMode(Protocol):
    """Execute one configured generator operating mode."""

    def run(self) -> dict[str, object]:
        """Run the mode and return its quality summary."""
        ...


class BootstrapMode:
    """Generate the one-time historical playback source delivery."""

    def __init__(self, config_path: Path, settings: BatchRuntimeSettings) -> None:
        """Build the batch runner for bootstrap mode."""
        shared, batch = load_batch_config(config_path)
        writer = MinioObjectWriter(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_landing_bucket,
        )
        self._runner = BatchRunner(BatchDataGenerator(shared, batch), writer, shared, batch)

    def run(self) -> dict[str, object]:
        """Generate and upload bootstrap Parquet objects."""
        return self._runner.bootstrap()


class RecurringBatchMode:
    """Generate one configured recurring full-snapshot delivery."""

    def __init__(
        self, config_path: Path, delivery_id: str, settings: BatchRuntimeSettings
    ) -> None:
        """Build the batch runner and select the requested delivery."""
        shared, batch = load_batch_config(config_path)
        writer = MinioObjectWriter(
            settings.minio_endpoint,
            settings.minio_access_key,
            settings.minio_secret_key,
            settings.minio_landing_bucket,
        )
        self._runner = BatchRunner(BatchDataGenerator(shared, batch), writer, shared, batch)
        self._delivery = batch.delivery(delivery_id)

    def run(self) -> dict[str, object]:
        """Generate and upload all datasets in the selected delivery."""
        return self._runner.recurring_batch(self._delivery)


class StreamMode:
    """Generate and publish one isolated Kafka execution."""

    def __init__(
        self, config_path: Path, execution_id: str, settings: StreamRuntimeSettings
    ) -> None:
        """Build the deterministic generator and Kafka publisher."""
        shared, stream = load_stream_config(config_path)
        generator = StreamDataGenerator(shared, stream)
        publisher = KafkaEventPublisher(
            settings.kafka_bootstrap_servers,
            settings.schema_registry_url,
            stream.topic,
            stream.partitions,
            settings.playback_avro_schema_path,
        )
        self._runner = StreamRunner(
            generator, publisher, stream.pace_events, shared.cutover_timestamp
        )
        self._shared = shared
        self._stream = stream
        self._execution_id = execution_id

    def run(self) -> dict[str, object]:
        """Publish and summarize all configured stream messages."""
        generated = self._runner.run(self._execution_id)
        return build_stream_summary(
            self._shared, self._stream, self._execution_id, generated
        )


class GeneratorModeFactory:
    """Create exactly one of the approved generator modes."""

    @staticmethod
    def create(
        mode: str,
        config_path: Path,
        delivery_id: str | None = None,
        execution_id: str | None = None,
    ) -> GeneratorMode:
        """Return the runner for a validated command selection.

        Raises:
            ValueError: When required mode arguments are missing or the mode is unsupported.
        """
        if mode == "bootstrap":
            return BootstrapMode(config_path, BatchRuntimeSettings.from_environment())
        if mode == "recurring_batch":
            if delivery_id is None:
                raise ValueError("recurring_batch requires delivery_id")
            return RecurringBatchMode(
                config_path, delivery_id, BatchRuntimeSettings.from_environment()
            )
        if mode == "stream":
            if execution_id is None:
                raise ValueError("stream requires execution_id")
            return StreamMode(
                config_path, execution_id, StreamRuntimeSettings.from_environment()
            )
        raise ValueError(f"Unsupported generator mode: {mode}")
