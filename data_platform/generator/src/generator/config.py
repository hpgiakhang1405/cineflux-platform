"""Load and validate shared, batch, and stream generator configuration.

Reads: YAML files under the component ``config`` directory.
Writes: immutable Pydantic configuration models used by generator runners.
Runs: configuration loading from the command-line entrypoint.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    """Base model that rejects unknown configuration keys."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class SharedConfig(StrictModel):
    """Values shared by matching batch and stream scenarios."""

    seed: int
    cutover_timestamp: datetime
    timezone: Literal["UTC"]
    user_count: int = Field(gt=0)
    content_count: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_cutover_timezone(self) -> SharedConfig:
        """Require an explicit timezone on the cutover timestamp."""
        if self.cutover_timestamp.tzinfo is None:
            raise ValueError("cutover_timestamp must include a timezone")
        return self


class SchemaEvolutionConfig(StrictModel):
    """Describe the field introduced by a later recurring delivery."""

    dataset: Literal["content"]
    field_name: Literal["critic_score"]
    introduced_schema_version: int = Field(ge=2)
    non_null_rate: float = Field(ge=0.0, le=1.0)


class DeliveryConfig(StrictModel):
    """Identify one immutable recurring source delivery."""

    delivery_id: str = Field(pattern=r"^[a-z0-9_]+$")
    delivery_timestamp: datetime
    schema_version: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_delivery_timezone(self) -> DeliveryConfig:
        """Require an explicit timezone on each delivery timestamp."""
        if self.delivery_timestamp.tzinfo is None:
            raise ValueError("delivery_timestamp must include a timezone")
        return self


class BatchConfig(StrictModel):
    """Control bootstrap and recurring batch data generation."""

    scenario_name: str = Field(pattern=r"^[a-z0-9_]+$")
    shared_config: str
    history_days: int = Field(gt=0)
    base_playback_event_count: int = Field(gt=0)
    rows_per_file: int = Field(gt=0)
    compression: Literal["snappy"]
    bootstrap_prefix: str
    recurring_prefix: str
    duplicate_rate: float = Field(ge=0.0, lt=1.0)
    entity_change_rate: float = Field(ge=0.0, le=1.0)
    city_distribution: dict[str, float]
    genre_distribution: dict[str, float]
    schema_evolution: SchemaEvolutionConfig
    deliveries: tuple[DeliveryConfig, ...]

    @model_validator(mode="after")
    def validate_batch_scenario(self) -> BatchConfig:
        """Validate probability distributions and schema delivery coverage."""
        _validate_distribution("city_distribution", self.city_distribution)
        _validate_distribution("genre_distribution", self.genre_distribution)
        versions = {delivery.schema_version for delivery in self.deliveries}
        if 1 not in versions:
            raise ValueError("deliveries must include schema version 1")
        if self.schema_evolution.introduced_schema_version not in versions:
            raise ValueError("deliveries must include the introduced schema version")
        delivery_ids = [delivery.delivery_id for delivery in self.deliveries]
        if len(delivery_ids) != len(set(delivery_ids)):
            raise ValueError("delivery_id values must be unique")
        return self

    def delivery(self, delivery_id: str) -> DeliveryConfig:
        """Return one configured delivery by ID.

        Args:
            delivery_id: Stable delivery identifier from the YAML file.

        Returns:
            The matching delivery configuration.

        Raises:
            ValueError: When the delivery ID is not configured.
        """
        for delivery in self.deliveries:
            if delivery.delivery_id == delivery_id:
                return delivery
        raise ValueError(f"Unknown delivery_id: {delivery_id}")


class BurstConfig(StrictModel):
    """Control periodic stream traffic bursts."""

    cycle_seconds: int = Field(gt=0)
    duration_ratio: float = Field(gt=0.0, lt=1.0)
    multiplier: int = Field(gt=1)


class StreamConfig(StrictModel):
    """Control Kafka playback event generation."""

    scenario_name: str = Field(pattern=r"^[a-z0-9_]+$")
    shared_config: str
    topic: str = Field(pattern=r"^[a-z0-9._-]+$")
    partitions: int = Field(gt=0)
    base_event_count: int = Field(gt=0)
    base_rate_per_second: int = Field(gt=0)
    pace_events: bool
    duplicate_rate: float = Field(ge=0.0, lt=1.0)
    late_arrival_rate: float = Field(ge=0.0, le=1.0)
    late_arrival_min_seconds: int = Field(ge=0)
    late_arrival_max_seconds: int = Field(ge=0)
    out_of_order_rate: float = Field(ge=0.0, le=1.0)
    out_of_order_buffer_size: int = Field(ge=2)
    invalid_payload_rate: float = Field(ge=0.0, le=1.0)
    burst: BurstConfig

    @model_validator(mode="after")
    def validate_stream_scenario(self) -> StreamConfig:
        """Validate mutually dependent stream settings."""
        if self.late_arrival_min_seconds > self.late_arrival_max_seconds:
            raise ValueError("late arrival minimum cannot exceed maximum")
        if self.invalid_payload_rate + self.duplicate_rate >= 1.0:
            raise ValueError("invalid and duplicate rates leave no valid base events")
        return self


def load_batch_config(path: Path) -> tuple[SharedConfig, BatchConfig]:
    """Load one batch file and its referenced shared configuration.

    Args:
        path: Batch YAML path.

    Returns:
        Validated shared and batch configuration models.
    """
    batch = BatchConfig.model_validate(_load_yaml(path))
    shared = SharedConfig.model_validate(_load_yaml(path.parent / batch.shared_config))
    return shared, batch


def load_stream_config(path: Path) -> tuple[SharedConfig, StreamConfig]:
    """Load one stream file and its referenced shared configuration.

    Args:
        path: Stream YAML path.

    Returns:
        Validated shared and stream configuration models.
    """
    stream = StreamConfig.model_validate(_load_yaml(path))
    shared = SharedConfig.model_validate(_load_yaml(path.parent / stream.shared_config))
    return shared, stream


def _load_yaml(path: Path) -> object:
    """Read a YAML document and fail with the source path in the error."""
    try:
        with path.open(encoding="utf-8") as file:
            return yaml.safe_load(file)
    except OSError as exc:
        raise ValueError(f"Unable to read configuration: {path}") from exc


def _validate_distribution(name: str, distribution: dict[str, float]) -> None:
    """Validate a non-empty probability distribution that sums to one."""
    if not distribution:
        raise ValueError(f"{name} cannot be empty")
    if any(weight < 0.0 for weight in distribution.values()):
        raise ValueError(f"{name} cannot contain negative weights")
    if abs(sum(distribution.values()) - 1.0) > 1e-9:
        raise ValueError(f"{name} must sum to 1.0")
