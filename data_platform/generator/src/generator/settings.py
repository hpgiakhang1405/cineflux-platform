"""Load mode-specific endpoints and credentials from environment variables.

Reads: process environment variables documented in component and root templates.
Writes: immutable batch or stream settings used by external gateways.
Runs: the command-line composition root.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BatchRuntimeSettings:
    """MinIO settings required by bootstrap and recurring batch modes."""

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_landing_bucket: str

    @classmethod
    def from_environment(cls) -> BatchRuntimeSettings:
        """Load only the environment values required by batch modes."""
        return cls(
            minio_endpoint=_required("MINIO_ENDPOINT"),
            minio_access_key=_required("MINIO_ROOT_USER"),
            minio_secret_key=_required("MINIO_ROOT_PASSWORD"),
            minio_landing_bucket=_required("MINIO_LANDING_BUCKET"),
        )


@dataclass(frozen=True)
class StreamRuntimeSettings:
    """Kafka and Schema Registry settings required by stream mode."""

    kafka_bootstrap_servers: str
    schema_registry_url: str
    playback_avro_schema_path: Path

    @classmethod
    def from_environment(cls) -> StreamRuntimeSettings:
        """Load only the environment values required by stream mode."""
        return cls(
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            schema_registry_url=_required("SCHEMA_REGISTRY_URL"),
            playback_avro_schema_path=Path(_required("PLAYBACK_AVRO_SCHEMA_PATH")),
        )


def _required(name: str) -> str:
    """Return a non-empty environment value or raise a contextual error."""
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(f"Required environment variable is missing: {name}")
    return value
