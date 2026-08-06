"""Load strict YAML configuration for reproducible Flink runs.

Reads: versioned YAML files from the component config directory.
Writes: immutable Pydantic configuration models.
Runs: before the stream execution environment is created.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field


class StrictConfig(BaseModel):
    """Reject unknown keys so configuration drift fails at startup."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class KafkaConfig(StrictConfig):
    """Kafka source, consumer, and dead-letter topic configuration."""

    source_topic: str = Field(pattern=r"^[a-z0-9._-]+$")
    source_partitions: int = Field(gt=0)
    consumer_group: str = Field(pattern=r"^[a-z0-9._-]+$")
    dlq_topic: str = Field(pattern=r"^[a-z0-9._-]+$")
    dlq_partitions: int = Field(gt=0)
    startup_mode: Literal["earliest-offset", "latest-offset"]


class OutputConfig(StrictConfig):
    """PostgreSQL table names owned by this streaming job."""

    playback_metrics_table: str = Field(pattern=r"^[a-z0-9_]+$")
    content_popularity_table: str = Field(pattern=r"^[a-z0-9_]+$")


class ProcessingConfig(StrictConfig):
    """Event-time, deduplication, and operator parallelism controls."""

    parallelism: int = Field(gt=0)
    window_minutes: Literal[5]
    watermark_seconds: int = Field(ge=0)
    source_idle_timeout_seconds: int = Field(gt=0)
    late_handling_enabled: bool
    dedup_enabled: bool
    dedup_ttl_minutes: int = Field(gt=0)


class CheckpointConfig(StrictConfig):
    """Checkpoint cadence and backpressure behavior."""

    interval_ms: int = Field(gt=0)
    timeout_ms: int = Field(gt=0)
    min_pause_ms: int = Field(ge=0)
    max_concurrent: int = Field(gt=0)
    tolerable_failures: int = Field(ge=0)
    unaligned_enabled: bool


class StateConfig(StrictConfig):
    """State backend selected for one benchmark run."""

    backend: Literal["hashmap", "rocksdb"]
    incremental: bool


class JdbcConfig(StrictConfig):
    """JDBC sink buffering and retry controls."""

    buffer_flush_max_rows: int = Field(gt=0)
    buffer_flush_interval_ms: int = Field(gt=0)
    max_retries: int = Field(ge=0)


class FlinkJobConfig(StrictConfig):
    """Complete configuration for one baseline or optimized run."""

    job_name: str = Field(pattern=r"^[a-z0-9-]+$")
    kafka: KafkaConfig
    outputs: OutputConfig
    processing: ProcessingConfig
    checkpoint: CheckpointConfig
    state: StateConfig
    jdbc: JdbcConfig
    restart_attempts: int = Field(ge=0)
    restart_delay_ms: int = Field(ge=0)


def load_job_config(path: Path) -> FlinkJobConfig:
    """Read and validate one YAML job configuration."""
    with path.open(encoding="utf-8") as stream:
        values = yaml.safe_load(stream)
    if not isinstance(values, dict):
        raise ValueError(f"Configuration root must be a mapping: {path}")
    return FlinkJobConfig.model_validate(values)
