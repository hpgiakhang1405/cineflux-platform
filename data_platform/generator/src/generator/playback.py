"""Derive stable playback-session attributes from a configured seed.

Reads: session indexes, entity ranges, and the shared generator seed.
Writes: deterministic user/content assignments and bounded numeric values.
Runs: historical and streaming playback event generation.
"""

from dataclasses import dataclass

MASK_64 = (1 << 64) - 1


@dataclass(frozen=True)
class SessionEntities:
    """Stable user and content indexes for one playback session."""

    user_index: int
    content_index: int


def session_entities(
    seed: int, session_index: int, user_count: int, content_count: int
) -> SessionEntities:
    """Assign one user and one content item to an entire playback session."""
    return SessionEntities(
        user_index=stable_int(seed, session_index, user_count, salt=11),
        content_index=stable_int(seed, session_index, content_count, salt=17),
    )


def stable_int(seed: int, index: int, upper_bound: int, salt: int) -> int:
    """Return a deterministic, uniformly mixed integer below ``upper_bound``."""
    if upper_bound <= 0:
        raise ValueError("upper_bound must be positive")
    value = (seed + index + salt * 0x9E3779B97F4A7C15) & MASK_64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK_64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK_64
    value ^= value >> 31
    return value % upper_bound
