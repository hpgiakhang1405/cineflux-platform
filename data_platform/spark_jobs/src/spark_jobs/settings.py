"""Load shared Spark, Hive, and MinIO runtime settings.

Reads: environment variables supplied locally or by Docker Compose.
Writes: a validated runtime settings object without logging secrets.
Runs: before building the Spark session.
"""

from datetime import datetime
from os import environ

from pydantic import BaseModel, ConfigDict, field_validator


class RuntimeSettings(BaseModel):
    """Connection settings required by Spark and the Iceberg catalog."""

    model_config = ConfigDict(frozen=True)

    spark_master_url: str
    hive_metastore_uri: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    landing_bucket: str
    warehouse_bucket: str
    cutover_timestamp: datetime
    catalog_name: str

    @field_validator("cutover_timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Require an explicit timezone for the batch/stream cutover."""
        if value.tzinfo is None:
            raise ValueError("CUTOVER_TIMESTAMP must include a timezone")
        return value

    @classmethod
    def from_environment(cls) -> "RuntimeSettings":
        """Build settings from required environment variables."""
        names = {
            "spark_master_url": "SPARK_MASTER_URL",
            "hive_metastore_uri": "HIVE_METASTORE_URI",
            "minio_endpoint": "MINIO_ENDPOINT",
            "minio_access_key": "MINIO_ROOT_USER",
            "minio_secret_key": "MINIO_ROOT_PASSWORD",
            "landing_bucket": "MINIO_LANDING_BUCKET",
            "warehouse_bucket": "MINIO_WAREHOUSE_BUCKET",
            "cutover_timestamp": "CUTOVER_TIMESTAMP",
            "catalog_name": "ICEBERG_CATALOG_NAME",
        }
        missing = [variable for variable in names.values() if not environ.get(variable)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        return cls.model_validate({field: environ[variable] for field, variable in names.items()})
