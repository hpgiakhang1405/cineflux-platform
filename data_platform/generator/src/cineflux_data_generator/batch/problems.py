"""Apply configurable offline data problems to generated record batches.

Reads: clean source records plus seeded NumPy random generators.
Writes: records with deterministic skew assignments or exact duplicate copies.
Runs: batch data generators before Parquet serialization.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.random import Generator


@dataclass(frozen=True)
class WeightedCategorySampler:
    """Apply a configured categorical distribution to generated entity rows."""

    distribution: dict[str, float]

    def sample(self, size: int, rng: Generator) -> np.ndarray[Any, np.dtype[np.str_]]:
        """Sample ``size`` labels with the configured weights."""
        labels = np.array(tuple(self.distribution.keys()))
        probabilities = np.array(tuple(self.distribution.values()), dtype=float)
        return rng.choice(labels, size=size, p=probabilities)


@dataclass(frozen=True)
class DuplicateInjector:
    """Apply exact duplicate rows at a configured final-row rate."""

    rate: float

    def apply(
        self, records: list[dict[str, Any]], rng: Generator
    ) -> list[dict[str, Any]]:
        """Return original rows plus shuffled exact copies."""
        if not records or self.rate == 0.0:
            return list(records)
        duplicate_count = round(len(records) * self.rate / (1.0 - self.rate))
        indexes = rng.integers(0, len(records), size=duplicate_count)
        duplicates = [dict(records[int(index)]) for index in indexes]
        combined = [*records, *duplicates]
        rng.shuffle(combined)
        return combined
