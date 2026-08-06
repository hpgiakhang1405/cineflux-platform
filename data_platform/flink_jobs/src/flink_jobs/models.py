"""Define the validated playback event contract and contract failures.

Reads: dictionaries decoded from Confluent-wire Avro payloads.
Writes: immutable playback event models used by Flink operators.
Runs: inside the contract-validation operator.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class PlaybackEvent(BaseModel):
    """One playback event accepted by the version-one stream contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    content_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    event_type: Literal["playback_started", "playback_progressed", "playback_completed"]
    position_seconds: int = Field(ge=0)
    watch_seconds: int = Field(ge=0)
    event_timestamp: int = Field(ge=0)
    produced_timestamp: int = Field(ge=0)
    payload_schema_version: Literal[1]


class ContractViolation(ValueError):
    """Describe one payload failure with a stable machine-readable code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
