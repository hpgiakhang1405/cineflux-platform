"""Build machine-readable quality summaries for generated batch data.

Reads: generated records, serialized Parquet bytes, and destination write results.
Writes: deterministic JSON-compatible dictionaries for terminal evidence.
Runs: automatically after bootstrap and recurring batch generation.
"""

from __future__ import annotations

import hashlib
import math
from collections import Counter
from dataclasses import dataclass

from cineflux_data_generator.batch.generator import GeneratedObject
from cineflux_data_generator.config import BatchConfig, DeliveryConfig, SharedConfig

DATASET_KEYS = {
    "initial_historical_playback_events": "event_id",
    "users": "user_id",
    "subscriptions": "user_id",
    "content": "content_id",
}


@dataclass
class DatasetMetrics:
    """Mutable counters for one generated dataset."""

    rows: int = 0
    files: int = 0
    bytes: int = 0
    uploaded_files: int = 0
    duplicate_rows: int = 0


class ApproxDistinctCounter:
    """Estimate cardinality with a compact deterministic HyperLogLog sketch."""

    def __init__(self, precision: int = 14) -> None:
        """Create a HyperLogLog sketch with ``2**precision`` registers."""
        self._precision = precision
        self._registers = bytearray(1 << precision)

    def add(self, value: object) -> None:
        """Add one value to the cardinality sketch."""
        hashed = int.from_bytes(
            hashlib.blake2b(str(value).encode(), digest_size=8).digest(), "big"
        )
        index = hashed & ((1 << self._precision) - 1)
        remaining = hashed >> self._precision
        width = 64 - self._precision
        rank = width - remaining.bit_length() + 1 if remaining else width + 1
        self._registers[index] = max(self._registers[index], rank)

    def estimate(self) -> int:
        """Return the rounded HyperLogLog cardinality estimate."""
        bucket_count = len(self._registers)
        alpha = 0.7213 / (1.0 + 1.079 / bucket_count)
        estimate = alpha * bucket_count * bucket_count / sum(
            2.0 ** -register for register in self._registers
        )
        zero_count = self._registers.count(0)
        if estimate <= 2.5 * bucket_count and zero_count:
            estimate = bucket_count * math.log(bucket_count / zero_count)
        return round(estimate)


class BatchSummaryCollector:
    """Collect bounded metrics while batch objects are serialized and written."""

    def __init__(
        self,
        mode: str,
        shared: SharedConfig,
        batch: BatchConfig,
        bucket: str,
        delivery: DeliveryConfig | None = None,
    ) -> None:
        """Initialize counters for one bootstrap or recurring delivery."""
        self._mode = mode
        self._shared = shared
        self._batch = batch
        self._bucket = bucket
        self._delivery = delivery
        self._datasets: dict[str, DatasetMetrics] = {}
        self._cities: Counter[str] = Counter()
        self._genres: Counter[str] = Counter()
        self._cardinality = {
            name: ApproxDistinctCounter() for name in ("user_id", "session_id", "event_id")
        }
        self._cutover_violations = 0
        self._schema_rows = 0
        self._schema_present_rows = 0
        self._schema_nulls_after_merge = 0

    def observe(self, generated: GeneratedObject, data: bytes, uploaded: bool) -> None:
        """Add one generated Parquet object to the summary."""
        metrics = self._datasets.setdefault(generated.dataset, DatasetMetrics())
        metrics.rows += len(generated.records)
        metrics.files += 1
        metrics.bytes += len(data)
        metrics.uploaded_files += int(uploaded)
        key = DATASET_KEYS[generated.dataset]
        metrics.duplicate_rows += len(generated.records) - len(
            {record[key] for record in generated.records}
        )

        if generated.dataset == "initial_historical_playback_events":
            for record in generated.records:
                for field, counter in self._cardinality.items():
                    counter.add(record[field])
                self._cutover_violations += int(
                    record["event_timestamp"] >= self._shared.cutover_timestamp
                )
        elif generated.dataset == "users":
            self._cities.update(str(record["city"]) for record in generated.records)
        elif generated.dataset == "content":
            self._genres.update(str(record["genres"][0]) for record in generated.records)
            field_name = self._batch.schema_evolution.field_name
            for record in generated.records:
                self._schema_rows += 1
                self._schema_present_rows += int(field_name in record)
                self._schema_nulls_after_merge += int(
                    field_name not in record or record[field_name] is None
                )

    def report(self) -> dict[str, object]:
        """Build the final JSON-compatible summary."""
        quality: dict[str, object] = {"duplicates": self._duplicate_summary()}
        if self._mode == "bootstrap":
            quality["cutover"] = {
                "rule": f"event_timestamp < {self._shared.cutover_timestamp.isoformat()}",
                "violations": self._cutover_violations,
            }
            quality["approx_count_distinct"] = {
                field: counter.estimate() for field, counter in self._cardinality.items()
            }
        else:
            quality["city_distribution"] = _distribution_summary(
                self._cities, self._batch.city_distribution
            )
            quality["genre_distribution"] = _distribution_summary(
                self._genres, self._batch.genre_distribution
            )
            quality["schema_evolution"] = self._schema_evolution_summary()

        report: dict[str, object] = {
            "summary_version": 1,
            "mode": self._mode,
            "scenario": self._batch.scenario_name,
            "quality": quality,
            "storage": self._storage_summary(),
        }
        if self._delivery is not None:
            report["delivery_id"] = self._delivery.delivery_id
            report["schema_version"] = self._delivery.schema_version
        return report

    def _schema_evolution_summary(self) -> dict[str, object]:
        """Describe the generated content schema for the selected delivery."""
        if self._delivery is None:
            return {}
        return {
            "delivery_id": self._delivery.delivery_id,
            "schema_version": self._delivery.schema_version,
            "field": self._batch.schema_evolution.field_name,
            "field_present": self._schema_present_rows == self._schema_rows,
            "rows": self._schema_rows,
            "null_count_after_schema_merge": self._schema_nulls_after_merge,
            "null_rate_after_schema_merge": _rate(
                self._schema_nulls_after_merge, self._schema_rows
            ),
        }

    def _duplicate_summary(self) -> list[dict[str, object]]:
        """Report duplicate rates before and after key deduplication."""
        summaries: list[dict[str, object]] = []
        for dataset, metrics in self._datasets.items():
            summaries.append(
                {
                    "dataset": dataset,
                    "key": DATASET_KEYS[dataset],
                    "configured_rate": self._batch.duplicate_rate,
                    "observed_rate": _rate(metrics.duplicate_rows, metrics.rows),
                    "rows_before_dedup": metrics.rows,
                    "duplicate_rows_before_dedup": metrics.duplicate_rows,
                    "rows_after_dedup": metrics.rows - metrics.duplicate_rows,
                    "duplicate_rows_after_dedup": 0,
                }
            )
        return summaries

    def _storage_summary(self) -> dict[str, object]:
        """Report actual serialized file counts and bytes for this command."""
        datasets = [
            {
                "dataset": dataset,
                "rows": metrics.rows,
                "files": metrics.files,
                "bytes": metrics.bytes,
                "uploaded_files": metrics.uploaded_files,
                "format": "parquet",
                "compression": self._batch.compression,
            }
            for dataset, metrics in self._datasets.items()
        ]
        return {
            "bucket": self._bucket,
            "rows": sum(item.rows for item in self._datasets.values()),
            "files": sum(item.files for item in self._datasets.values()),
            "bytes": sum(item.bytes for item in self._datasets.values()),
            "uploaded_files": sum(item.uploaded_files for item in self._datasets.values()),
            "datasets": datasets,
        }


def _distribution_summary(
    observed: Counter[str], configured: dict[str, float]
) -> list[dict[str, object]]:
    """Compare observed categorical percentages with configured weights."""
    total = sum(observed.values())
    return [
        {
            "value": value,
            "configured_rate": configured_rate,
            "observed_count": observed[value],
            "observed_rate": _rate(observed[value], total),
        }
        for value, configured_rate in configured.items()
    ]


def _rate(numerator: int, denominator: int) -> float:
    """Return a stable six-decimal rate with zero-safe division."""
    return round(numerator / denominator, 6) if denominator else 0.0
