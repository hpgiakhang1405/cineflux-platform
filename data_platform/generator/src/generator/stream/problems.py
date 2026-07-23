"""Select deterministic streaming problems and calculate burst schedules.

Reads: stream rate configuration and a seeded NumPy random generator.
Writes: selected event indexes and simulated production timestamps.
Runs: StreamDataGenerator initialization and event generation.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from generator.config import BurstConfig


@dataclass(frozen=True)
class RateIndexSelector:
    """Select a configured fraction of unique event indexes."""

    rate: float
    excluded: frozenset[int] = frozenset()

    def select(self, population_size: int, rng: np.random.Generator) -> set[int]:
        """Return sampled indexes excluding incompatible problem rows."""
        candidates = np.array(
            [index for index in range(population_size) if index not in self.excluded]
        )
        count = min(round(population_size * self.rate), len(candidates))
        return _sample(candidates, count, rng)


@dataclass(frozen=True)
class DuplicateIndexSelector:
    """Select retry sources so duplicates match a final-message rate."""

    final_rate: float
    excluded: frozenset[int]

    def select(self, population_size: int, rng: np.random.Generator) -> set[int]:
        """Return unique valid source indexes that will be published twice."""
        candidates = np.array(
            [index for index in range(population_size) if index not in self.excluded]
        )
        count = round(population_size * self.final_rate / (1.0 - self.final_rate))
        return _sample(candidates, min(count, len(candidates)), rng)


@dataclass(frozen=True)
class OutOfOrderIndexSelector:
    """Select non-adjacent swap anchors inside each reorder buffer."""

    rate: float
    buffer_size: int

    def select(self, population_size: int, rng: np.random.Generator) -> set[int]:
        """Return indexes that are guaranteed to have a following swap partner."""
        candidates = np.array(
            [
                index
                for buffer_start in range(0, population_size, self.buffer_size)
                for index in range(
                    buffer_start,
                    min(buffer_start + self.buffer_size, population_size) - 1,
                    2,
                )
            ]
        )
        count = min(round(population_size * self.rate), len(candidates))
        return _sample(candidates, count, rng)


@dataclass(frozen=True)
class ScheduledTime:
    """Simulated event production time and burst membership."""

    offset_seconds: float
    is_burst: bool


class BurstSchedule:
    """Map event indexes onto periodic base-rate and burst-rate intervals."""

    def __init__(self, base_rate: int, burst: BurstConfig) -> None:
        """Create a deterministic periodic traffic schedule."""
        self._base_rate = base_rate
        self._burst_rate = base_rate * burst.multiplier
        self._cycle_seconds = burst.cycle_seconds
        self._burst_seconds = burst.cycle_seconds * burst.duration_ratio
        self._normal_seconds = self._cycle_seconds - self._burst_seconds
        self._burst_capacity = max(1, round(self._burst_rate * self._burst_seconds))
        self._normal_capacity = max(1, round(self._base_rate * self._normal_seconds))
        self._cycle_capacity = self._burst_capacity + self._normal_capacity

    def at(self, index: int) -> ScheduledTime:
        """Return the simulated production offset for a base event index."""
        cycle_index, within_cycle = divmod(index, self._cycle_capacity)
        cycle_start = cycle_index * self._cycle_seconds
        if within_cycle < self._burst_capacity:
            return ScheduledTime(
                offset_seconds=cycle_start + within_cycle / self._burst_rate,
                is_burst=True,
            )
        normal_index = within_cycle - self._burst_capacity
        return ScheduledTime(
            offset_seconds=cycle_start + self._burst_seconds + normal_index / self._base_rate,
            is_burst=False,
        )


def _sample(
    candidates: np.ndarray, count: int, rng: np.random.Generator
) -> set[int]:
    """Sample unique integer candidates without replacement."""
    if count == 0:
        return set()
    return {int(index) for index in rng.choice(candidates, size=count, replace=False)}
