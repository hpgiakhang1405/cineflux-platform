"""Expose DP1 and DP2 as explicit Spark command-line jobs.

Reads: a YAML config path, pipeline run ID, and runtime environment variables.
Writes: one JSON run summary to stdout and operational logs to stderr.
Runs: through `spark-submit` in Docker Compose or a future Airflow task.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from spark_jobs.compact_iceberg import IcebergCompactionJob
from spark_jobs.config import (
    load_dp1_config,
    load_dp2_config,
    load_iceberg_compaction_config,
)
from spark_jobs.dp1_ingest_raw import Dp1IngestRawJob
from spark_jobs.dp2_bronze_to_silver import Dp2BronzeToSilverJob
from spark_jobs.settings import RuntimeSettings
from spark_jobs.spark_session import create_spark_session


def _parser() -> argparse.ArgumentParser:
    """Build the Spark jobs command-line parser."""
    parser = argparse.ArgumentParser(prog="cineflux-spark-jobs")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("dp1", "dp2", "compact"):
        subparser = commands.add_parser(command)
        subparser.add_argument("--config", type=Path, required=True)
        subparser.add_argument("--run-id", required=True)
    return parser


def main() -> None:
    """Run the selected Spark job and print its structured summary."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    arguments = _parser().parse_args()
    settings = RuntimeSettings.from_environment()
    if arguments.command == "dp1":
        config = load_dp1_config(arguments.config)
    elif arguments.command == "dp2":
        config = load_dp2_config(arguments.config)
    else:
        config = load_iceberg_compaction_config(arguments.config)
    spark = create_spark_session(config.job_name, settings, config.spark)
    try:
        if arguments.command == "dp1":
            job = Dp1IngestRawJob(spark, arguments.run_id, config, settings)
        elif arguments.command == "dp2":
            job = Dp2BronzeToSilverJob(spark, arguments.run_id, config, settings)
        else:
            job = IcebergCompactionJob(spark, arguments.run_id, config, settings)
        print(json.dumps(job.run(), indent=2, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
