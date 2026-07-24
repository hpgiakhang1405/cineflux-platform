"""Read source-aligned Parquet deliveries from MinIO landing storage.

Reads: immutable generator objects through the S3A filesystem.
Writes: Spark DataFrames with the requested physical schema behavior.
Runs: DP1 landing ingestion.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class LandingRepository:
    """Gateway for Parquet objects in one MinIO landing bucket."""

    def __init__(self, spark: SparkSession, bucket: str) -> None:
        """Create the gateway for an existing landing bucket."""
        self._spark = spark
        self._bucket = bucket

    def read(self, prefix: str, schema: StructType | None, merge_schema: bool) -> DataFrame:
        """Read one landing prefix with explicit or merged Parquet schema."""
        reader = self._spark.read.option("mergeSchema", str(merge_schema).lower())
        if schema is not None:
            reader = reader.schema(schema)
        return reader.parquet(f"s3a://{self._bucket}/{prefix.strip('/')}")
