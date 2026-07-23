"""Create deterministic high-cardinality CineFlux identifiers.

Reads: zero-based entity and event indexes.
Writes: stable string identifiers used by batch and streaming records.
Runs: batch and stream record generation.
"""


def user_id(index: int) -> str:
    """Return a stable user identifier for a zero-based index."""
    return f"usr_{index + 1:012d}"


def content_id(index: int) -> str:
    """Return a stable content identifier for a zero-based index."""
    return f"cnt_{index + 1:010d}"


def session_id(index: int) -> str:
    """Return a stable playback session identifier for a zero-based index."""
    return f"ses_{index + 1:014d}"


def event_id(index: int) -> str:
    """Return a stable playback event identifier for a zero-based index."""
    return f"evt_{index + 1:016d}"
