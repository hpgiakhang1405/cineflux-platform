"""Load runtime endpoints and credentials from environment variables.

Reads: Docker Compose environment variables.
Writes: an immutable runtime settings object.
Runs: once on the Flink client before job submission.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RuntimeSettings:
    """External service locations and schema paths required by the job."""

    kafka_bootstrap_servers: str
    schema_registry_url: str
    postgres_endpoint: str
    postgres_database: str
    postgres_user: str
    postgres_password: str
    postgres_streaming_schema: str
    minio_checkpoint_bucket: str
    playback_schema_path: Path
    dlq_schema_path: Path
    python_executable: str

    @classmethod
    def from_environment(cls) -> RuntimeSettings:
        """Build settings while failing clearly for every missing variable."""
        return cls(
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            schema_registry_url=_required("SCHEMA_REGISTRY_URL").rstrip("/"),
            postgres_endpoint=_required("POSTGRES_ENDPOINT"),
            postgres_database=_required("CINEFLUX_POSTGRES_DB"),
            postgres_user=_required("POSTGRES_USER"),
            postgres_password=_required("POSTGRES_PASSWORD"),
            postgres_streaming_schema=_required("POSTGRES_STREAMING_SCHEMA"),
            minio_checkpoint_bucket=_required("MINIO_CHECKPOINT_BUCKET"),
            playback_schema_path=Path(_required("PLAYBACK_AVRO_SCHEMA_PATH")),
            dlq_schema_path=Path(_required("PLAYBACK_DLQ_AVRO_SCHEMA_PATH")),
            python_executable=_required("PYFLINK_PYTHON_EXECUTABLE"),
        )

    @property
    def jdbc_url(self) -> str:
        """Return the internal PostgreSQL JDBC URL."""
        return f"jdbc:postgresql://{self.postgres_endpoint}/{self.postgres_database}"

    def checkpoint_uri(self, job_name: str) -> str:
        """Return an isolated checkpoint prefix for one named job."""
        return f"s3://{self.minio_checkpoint_bucket}/flink/{job_name}"


def _required(name: str) -> str:
    """Read one non-empty environment variable."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value
