"""Expose DP1 and DP2 as explicit Spark command-line jobs.

Reads: a YAML config path, pipeline run ID, and runtime environment variables.
Writes: one JSON run summary to stdout and operational logs to stderr.
Runs: through `spark-submit` in Docker Compose or Airflow.
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
    load_iceberg_snapshot_expiration_config,
)
from spark_jobs.dp1_ingest_raw import Dp1IngestRawJob
from spark_jobs.dp2_bronze_to_silver import Dp2BronzeToSilverJob
from spark_jobs.expire_iceberg_snapshots import IcebergSnapshotExpirationJob
from spark_jobs.settings import RuntimeSettings
from spark_jobs.spark_session import create_spark_session


def _parser() -> argparse.ArgumentParser:
    """Build the Spark jobs command-line parser."""
    parser = argparse.ArgumentParser(prog="cineflux-spark-jobs")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("dp1", "dp2", "compact", "expire-snapshots"):
        subparser = commands.add_parser(command)
        subparser.add_argument("--config", type=Path, required=True)
        subparser.add_argument("--run-id", required=True)
        subparser.add_argument("--namespace")
        if command in {"dp1", "dp2"}:
            subparser.add_argument("--validate-only", action="store_true")
        if command == "dp1":
            subparser.add_argument("--landing-scenario")
            subparser.add_argument("--bootstrap-prefix")
            subparser.add_argument("--recurring-prefix")
        if command == "dp2":
            subparser.add_argument("--silver-namespace")
    return parser


def _apply_overrides(arguments: argparse.Namespace, config: object) -> object:
    """Apply orchestration-owned namespace and landing overrides."""
    if arguments.command == "dp1":
        landing_updates = {
            key: value
            for key, value in {
                "scenario": arguments.landing_scenario,
                "bootstrap_prefix": arguments.bootstrap_prefix,
                "recurring_prefix": arguments.recurring_prefix,
            }.items()
            if value is not None
        }
        updates: dict[str, object] = {}
        if landing_updates:
            updates["landing"] = config.landing.model_copy(update=landing_updates)
        if arguments.namespace:
            updates["bronze"] = config.bronze.model_copy(
                update={"namespace": arguments.namespace}
            )
        return config.model_copy(update=updates)
    if arguments.command == "dp2":
        updates = {}
        if arguments.namespace:
            updates["bronze"] = config.bronze.model_copy(
                update={"namespace": arguments.namespace}
            )
        if arguments.silver_namespace:
            updates["silver"] = config.silver.model_copy(
                update={"namespace": arguments.silver_namespace}
            )
        return config.model_copy(update=updates)
    if arguments.namespace:
        return config.model_copy(
            update={"table": config.table.model_copy(update={"namespace": arguments.namespace})}
        )
    return config


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
    elif arguments.command == "compact":
        config = load_iceberg_compaction_config(arguments.config)
    else:
        config = load_iceberg_snapshot_expiration_config(arguments.config)
    config = _apply_overrides(arguments, config)
    spark = create_spark_session(config.job_name, settings, config.spark)
    try:
        if arguments.command == "dp1":
            job = Dp1IngestRawJob(spark, arguments.run_id, config, settings)
        elif arguments.command == "dp2":
            job = Dp2BronzeToSilverJob(spark, arguments.run_id, config, settings)
        elif arguments.command == "compact":
            job = IcebergCompactionJob(spark, arguments.run_id, config, settings)
        else:
            job = IcebergSnapshotExpirationJob(
                spark, arguments.run_id, config, settings
            )
        validate_only = getattr(arguments, "validate_only", False)
        result = job.validate() if validate_only else job.run()
        print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
