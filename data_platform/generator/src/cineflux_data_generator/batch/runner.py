"""Run batch generation and write Parquet objects to landing storage.

Reads: BatchDataGenerator objects and an injected ObjectWriter gateway.
Writes: immutable Parquet files plus structured generation logs.
Runs: ``bootstrap`` and ``recurring_batch`` command modes.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from cineflux_data_generator.batch.generator import BatchDataGenerator, GeneratedObject
from cineflux_data_generator.batch.summary import BatchSummaryCollector
from cineflux_data_generator.config import BatchConfig, DeliveryConfig, SharedConfig
from cineflux_data_generator.io.parquet import serialize_parquet
from cineflux_data_generator.io.protocols import ObjectWriter

LOGGER = logging.getLogger(__name__)


class BatchRunner:
    """Coordinate pure batch generation with Parquet and MinIO gateways."""

    def __init__(
        self,
        generator: BatchDataGenerator,
        store: ObjectWriter,
        shared: SharedConfig,
        batch: BatchConfig,
    ) -> None:
        """Create a batch runner with explicit dependencies."""
        self._generator = generator
        self._store = store
        self._shared = shared
        self._batch = batch

    def bootstrap(self) -> dict[str, object]:
        """Generate and store the one-time historical playback delivery."""
        collector = BatchSummaryCollector(
            "bootstrap", self._shared, self._batch, self._store.bucket
        )
        self._write_objects(self._generator.bootstrap_objects(), collector)
        return collector.report()

    def recurring_batch(self, delivery: DeliveryConfig) -> dict[str, object]:
        """Generate and store one configured recurring full snapshot."""
        collector = BatchSummaryCollector(
            "recurring_batch", self._shared, self._batch, self._store.bucket, delivery
        )
        self._write_objects(self._generator.recurring_objects(delivery), collector)
        return collector.report()

    def _write_objects(
        self, objects: Iterable[GeneratedObject], collector: BatchSummaryCollector
    ) -> None:
        """Serialize and write objects while reporting row and upload counts."""
        total_rows = 0
        total_files = 0
        uploaded_files = 0
        for generated in objects:
            data = serialize_parquet(generated.records, self._batch.compression)
            uploaded = self._store.write(
                generated.object_name, data, "application/vnd.apache.parquet"
            )
            collector.observe(generated, data, uploaded)
            total_rows += len(generated.records)
            total_files += 1
            uploaded_files += int(uploaded)
            LOGGER.info(
                "batch_file_written dataset=%s object=%s rows=%d bytes=%d uploaded=%s",
                generated.dataset,
                generated.object_name,
                len(generated.records),
                len(data),
                uploaded,
            )
        LOGGER.info(
            "batch_generation_completed rows=%d files=%d uploaded_files=%d",
            total_rows,
            total_files,
            uploaded_files,
        )
