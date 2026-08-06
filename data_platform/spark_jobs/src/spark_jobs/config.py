"""Load validated YAML configuration for CineFlux Spark jobs.

Reads: versioned YAML files from the component config directory.
Writes: immutable Pydantic configuration models.
Runs: before a Spark session is created for DP1 or DP2.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field


class StrictConfig(BaseModel):
    """Reject unknown configuration fields to prevent silent drift."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class SparkTuning(StrictConfig):
    """Spark settings shared by one reproducible job run."""

    shuffle_partitions: int = Field(gt=0)
    adaptive_enabled: bool
    broadcast_threshold_bytes: int


class LandingPaths(StrictConfig):
    """Landing object prefixes selected for one generator scenario."""

    scenario: str = Field(pattern=r"^[a-z0-9_]+$")
    bootstrap_prefix: str
    recurring_prefix: str


class BronzeTables(StrictConfig):
    """Bronze namespace and raw table names."""

    namespace: str = Field(pattern=r"^[a-z0-9_]+$")
    users: str = "raw_users"
    subscriptions: str = "raw_subscriptions"
    content: str = "raw_content"
    playback_events: str = "raw_playback_events"


class SilverTables(StrictConfig):
    """Silver namespace and staging table names."""

    namespace: str = Field(pattern=r"^[a-z0-9_]+$")
    users: str = "stg_users"
    subscriptions: str = "stg_subscriptions"
    content: str = "stg_content"
    playback_events: str = "stg_playback_events"
    playback_sessions: str = "int_playback_sessions"


class Dp1Config(StrictConfig):
    """Complete configuration for landing-to-Bronze ingestion."""

    job_name: str
    spark: SparkTuning
    landing: LandingPaths
    bronze: BronzeTables
    schema_strategy: Literal["merge", "explicit"]
    source_system: str
    playback_write_batches: int = Field(default=1, gt=0)


class Dp2Config(StrictConfig):
    """Complete configuration for Bronze-to-Silver processing."""

    job_name: str
    spark: SparkTuning
    bronze: BronzeTables
    silver: SilverTables
    dedup_strategy: Literal["window"]
    session_shuffle_partitions: int = Field(gt=0)
    skew_strategy: Literal["direct", "salted"]
    skew_salt_buckets: int = Field(gt=1)
    completion_threshold: float = Field(ge=0.0, le=1.0)


class IcebergCompactionTable(StrictConfig):
    """Identify one Iceberg table and its validation columns."""

    namespace: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str = Field(pattern=r"^[a-z0-9_]+$")
    business_key: str = Field(pattern=r"^[a-z0-9_]+$")
    event_timestamp_column: str = Field(pattern=r"^[a-z0-9_]+$")


class IcebergCompactionConfig(StrictConfig):
    """Configure one reproducible Iceberg data-file rewrite."""

    job_name: str
    spark: SparkTuning
    table: IcebergCompactionTable
    strategy: Literal["binpack"]
    target_file_size_bytes: int = Field(gt=0)
    min_input_files: int = Field(gt=1)
    require_file_reduction: bool = True


class IcebergSnapshotExpirationTable(StrictConfig):
    """Identify one Iceberg table whose old snapshots can be expired."""

    namespace: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str = Field(pattern=r"^[a-z0-9_]+$")


class IcebergSnapshotExpirationConfig(StrictConfig):
    """Configure safe snapshot expiration for one Iceberg table."""

    job_name: str
    spark: SparkTuning
    table: IcebergSnapshotExpirationTable
    retention_hours: int = Field(gt=0)
    retain_last: int = Field(gt=0)


def _load_yaml(path: Path) -> dict[str, object]:
    """Read one YAML mapping from disk."""
    with path.open(encoding="utf-8") as stream:
        values = yaml.safe_load(stream)
    if not isinstance(values, dict):
        raise ValueError(f"Configuration root must be a mapping: {path}")
    return values


def load_dp1_config(path: Path) -> Dp1Config:
    """Load and validate a DP1 configuration file."""
    return Dp1Config.model_validate(_load_yaml(path))


def load_dp2_config(path: Path) -> Dp2Config:
    """Load and validate a DP2 configuration file."""
    return Dp2Config.model_validate(_load_yaml(path))


def load_iceberg_compaction_config(path: Path) -> IcebergCompactionConfig:
    """Load and validate an Iceberg compaction configuration file."""
    return IcebergCompactionConfig.model_validate(_load_yaml(path))


def load_iceberg_snapshot_expiration_config(
    path: Path,
) -> IcebergSnapshotExpirationConfig:
    """Load and validate an Iceberg snapshot-expiration configuration file."""
    return IcebergSnapshotExpirationConfig.model_validate(_load_yaml(path))
