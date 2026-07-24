"""Read and write Iceberg tables through Spark's catalog API.

Reads: Bronze and Silver tables registered in Hive Metastore.
Writes: append-only Bronze tables and atomically replaced Silver tables.
Runs: DP1 and DP2 persistence stages.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, days


class IcebergTableRepository:
    """Gateway for one named Spark Iceberg catalog."""

    def __init__(self, spark: SparkSession, catalog: str) -> None:
        """Create a repository using the configured shared catalog."""
        self._spark = spark
        self._catalog = catalog

    def qualified(self, namespace: str, table: str) -> str:
        """Return a fully qualified Spark table identifier."""
        return f"{self._catalog}.{namespace}.{table}"

    def ensure_namespace(self, namespace: str) -> None:
        """Create an Iceberg namespace when it does not exist."""
        self._spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self._catalog}.{namespace}")

    def exists(self, namespace: str, table: str) -> bool:
        """Return whether a catalog table already exists."""
        return self._spark.catalog.tableExists(self.qualified(namespace, table))

    def read(self, namespace: str, table: str) -> DataFrame:
        """Read one Iceberg table as a DataFrame."""
        return self._spark.table(self.qualified(namespace, table))

    def append(
        self,
        namespace: str,
        table: str,
        frame: DataFrame,
        partition_day_column: str | None = None,
    ) -> None:
        """Append a DataFrame, creating the Iceberg table on its first run."""
        target = self.qualified(namespace, table)
        if self.exists(namespace, table):
            frame.writeTo(target).append()
            return
        writer = frame.writeTo(target).using("iceberg").tableProperty("format-version", "2")
        if partition_day_column is not None:
            writer = writer.partitionedBy(days(col(partition_day_column)))
        writer.create()

    def replace(
        self,
        namespace: str,
        table: str,
        frame: DataFrame,
        partition_day_column: str | None = None,
    ) -> None:
        """Atomically replace a deterministic Silver table."""
        target = self.qualified(namespace, table)
        writer = frame.writeTo(target).using("iceberg").tableProperty("format-version", "2")
        if partition_day_column is not None:
            writer = writer.partitionedBy(days(col(partition_day_column)))
        writer.createOrReplace()
