"""Validate stream payloads and carry simulation metadata.

Reads: generated playback event values and problem flags.
Writes: Avro-compatible payload dictionaries and Kafka header metadata.
Runs: stream generation, publishing, and verification.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StreamEvent(BaseModel):
    """One valid playback event matching the versioned Avro contract."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    user_id: str
    content_id: str
    session_id: str
    event_type: Literal["playback_started", "playback_progressed", "playback_completed"]
    position_seconds: int = Field(ge=0)
    watch_seconds: int = Field(ge=0)
    event_timestamp: int = Field(ge=0)
    produced_timestamp: int = Field(ge=0)
    payload_schema_version: int = Field(ge=1)


@dataclass(frozen=True)
class StreamEnvelope:
    """A stream payload plus transport-only simulation truth metadata."""

    event: StreamEvent
    simulated_offset_seconds: float
    is_burst: bool
    is_late: bool
    is_out_of_order: bool
    is_duplicate: bool = False
    is_invalid: bool = False

    def duplicate(self) -> StreamEnvelope:
        """Return an exact event retry marked as a duplicate."""
        return replace(self, is_duplicate=True)
