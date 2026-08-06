"""Create a Spark session configured for Iceberg on MinIO.

Reads: runtime settings and versioned job tuning.
Writes: a configured SparkSession connected to Hive Metastore.
Runs: once per DP1 or DP2 command.
"""

from urllib.parse import urlparse

from pyspark.sql import SparkSession

from spark_jobs.config import SparkTuning
from spark_jobs.settings import RuntimeSettings


def create_spark_session(
    app_name: str, settings: RuntimeSettings, tuning: SparkTuning
) -> SparkSession:
    """Build the shared Spark and Iceberg runtime configuration."""
    endpoint = urlparse(settings.minio_endpoint)
    if endpoint.scheme not in {"http", "https"} or not endpoint.netloc:
        raise ValueError("MINIO_ENDPOINT must be a complete HTTP or HTTPS URL")

    catalog = settings.catalog_name
    warehouse = f"s3a://{settings.warehouse_bucket}/"
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master_url)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hive")
        .config(f"spark.sql.catalog.{catalog}.uri", settings.hive_metastore_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(tuning.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(tuning.adaptive_enabled).lower())
        .config(
            "spark.sql.autoBroadcastJoinThreshold", str(tuning.broadcast_threshold_bytes)
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.scheme == "https").lower())
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
