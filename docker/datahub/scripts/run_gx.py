"""Run the Bronze validation suite and publish its result through the DataHub GX action."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint


def required_environment(name: str) -> str:
    """Return one required environment variable or fail with a clear message."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def run_validation() -> dict[str, object]:
    """Validate the DP1 playback Bronze contract and publish GX assertions to DataHub."""
    bronze_schema = required_environment("AIRFLOW_BRONZE_SCHEMA")
    trino_url = (
        f"{required_environment('DATAHUB_TRINO_SCHEME')}://"
        f"{required_environment('DBT_TRINO_USER')}@"
        f"{required_environment('DBT_TRINO_HOST')}:"
        f"{required_environment('DBT_TRINO_PORT')}/"
        f"{required_environment('DBT_ICEBERG_CATALOG')}/{bronze_schema}"
    )
    context = gx.get_context(mode="ephemeral")
    datasource = context.sources.add_sql(
        name="cineflux_trino",
        connection_string=trino_url,
    )
    asset = datasource.add_table_asset(
        name="raw_playback_events",
        table_name="raw_playback_events",
        schema_name=bronze_schema,
    )
    suite = context.add_expectation_suite("dp1_bronze_playback_contract")
    validator = context.get_validator(
        batch_request=asset.build_batch_request(),
        expectation_suite=suite,
    )
    validator.expect_table_row_count_to_be_between(min_value=1)
    validator.expect_table_columns_to_match_set(
        column_set=[
            "event_id",
            "user_id",
            "content_id",
            "session_id",
            "event_type",
            "event_timestamp",
            "payload_schema_version",
            "_source_file",
            "_ingested_at",
            "_source_system",
            "_record_hash",
            "_pipeline_run_id",
        ],
        exact_match=False,
    )
    validator.expect_column_values_to_not_be_null("event_id")
    validator.expect_column_values_to_not_be_null("event_timestamp")
    validator.expect_column_values_to_be_in_set(
        "_source_system",
        ["cineflux_generator"],
    )
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = SimpleCheckpoint(
        name="dp1_bronze_playback_contract",
        data_context=context,
        validations=[
            {
                "batch_request": asset.build_batch_request(),
                "expectation_suite_name": suite.expectation_suite_name,
            }
        ],
        action_list=[
            {
                "name": "publish_to_datahub",
                "action": {
                    "module_name": "datahub_gx_plugin.action",
                    "class_name": "DataHubValidationAction",
                    "server_url": required_environment("DATAHUB_GMS_URL"),
                    "env": required_environment("DATAHUB_ENV"),
                    "platform_alias": "trino",
                    "platform_instance_map": {
                        "trino": required_environment(
                            "DATAHUB_TRINO_PLATFORM_INSTANCE"
                        )
                    },
                    "graceful_exceptions": False,
                },
            }
        ],
    )
    result = checkpoint.run()
    validation_results = list(result.run_results.values())
    if len(validation_results) != 1:
        raise RuntimeError("Expected exactly one Great Expectations validation result")
    suite_result = validation_results[0]["validation_result"]
    summary = {
        "success": bool(suite_result.success),
        "statistics": dict(suite_result.statistics),
        "suite": suite.expectation_suite_name,
    }
    if not summary["success"]:
        raise RuntimeError(f"Great Expectations validation failed: {summary}")
    return summary


def main() -> None:
    """Run validation and persist its compact result for contract publication."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = run_validation()
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
