"""Validate records written by the batch data generator.

Reads: generated user, subscription, content, and playback dictionaries.
Writes: typed Python dictionaries with timestamp and list values preserved for Parquet.
Runs: batch generator record validation before serialization.
"""

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class BatchRecord(BaseModel):
    """Base model for source records with strict field validation."""

    model_config = ConfigDict(extra="forbid")


class UserRecord(BatchRecord):
    """One recurring user profile version."""

    user_id: str
    country_code: str
    city: str
    birth_year: int = Field(ge=1900, le=2010)
    preferred_language: str
    marketing_opt_in: bool
    source_updated_timestamp: datetime


class SubscriptionRecord(BatchRecord):
    """One recurring subscription version."""

    user_id: str
    subscription_tier: Literal["basic", "standard", "premium"]
    subscription_status: Literal["active", "paused", "cancelled"]
    billing_country_code: str
    started_date: date
    ended_date: date | None
    source_updated_timestamp: datetime


class ContentRecordV1(BatchRecord):
    """Content source schema before critic scores are introduced."""

    content_id: str
    content_type: Literal["movie", "series", "documentary"]
    title: str
    genres: list[str]
    release_year: int = Field(ge=1920, le=2030)
    runtime_minutes: int = Field(gt=0)
    maturity_rating: str
    original_language: str
    availability_status: Literal["available", "unavailable"]
    source_updated_timestamp: datetime


class ContentRecordV2(ContentRecordV1):
    """Content source schema after critic scores are introduced."""

    critic_score: float | None = Field(default=None, ge=0.0, le=100.0)


class PlaybackEventRecord(BatchRecord):
    """One historical playback event written to the bootstrap landing path."""

    event_id: str
    user_id: str
    content_id: str
    session_id: str
    event_type: Literal["playback_started", "playback_progressed", "playback_completed"]
    position_seconds: int = Field(ge=0)
    watch_seconds: int = Field(ge=0)
    event_timestamp: datetime
    produced_timestamp: datetime
    payload_schema_version: int = Field(ge=1)
