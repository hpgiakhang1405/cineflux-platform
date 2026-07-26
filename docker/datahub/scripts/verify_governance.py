"""Verify the governance entities required for the three CineFlux batch pipelines."""

from __future__ import annotations

import hashlib
import json
import os

from datahub.emitter.mce_builder import (
    make_assertion_urn,
    make_data_flow_urn,
    make_data_job_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    DataContractPropertiesClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DomainsClass,
    GlobalTagsClass,
    OwnershipClass,
)


def required_environment(name: str) -> str:
    """Return one required environment variable or fail with a clear message."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def contract_urn(contract_id: str) -> str:
    """Build the same stable Data Contract URN used by the publisher."""
    digest = hashlib.sha256(contract_id.encode("utf-8")).hexdigest()[:32]
    return f"urn:li:dataContract:{digest}"


def dataset_urn(platform: str, name: str, instance_variable: str) -> str:
    """Build one canonical dataset URN from environment-owned platform settings."""
    return make_dataset_urn_with_platform_instance(
        platform,
        name,
        required_environment(instance_variable),
        required_environment("DATAHUB_ENV"),
    )


def main() -> None:
    """Fail unless pipelines, governed datasets, contracts, and passing runs exist."""
    graph = DataHubGraph(
        DatahubClientConfig(server=required_environment("DATAHUB_GMS_URL"))
    )
    iceberg_catalog = required_environment("DBT_ICEBERG_CATALOG")
    bronze_schema = required_environment("AIRFLOW_BRONZE_SCHEMA")
    feature_schema = required_environment("DBT_FEATURE_SCHEMA")
    postgres_database = required_environment("CINEFLUX_POSTGRES_DB")
    serving_schema = required_environment("POSTGRES_SERVING_SCHEMA")
    datasets = {
        "DP1 Bronze": dataset_urn(
            "trino",
            f"{iceberg_catalog}.{bronze_schema}.raw_playback_events",
            "DATAHUB_TRINO_PLATFORM_INSTANCE",
        ),
        "DP2 serving": dataset_urn(
            "postgres",
            f"{postgres_database}.{serving_schema}.mart_content_trending",
            "DATAHUB_POSTGRES_PLATFORM_INSTANCE",
        ),
        "DP3 user features": dataset_urn(
            "trino",
            f"{iceberg_catalog}.{feature_schema}.feat_user_engagement",
            "DATAHUB_TRINO_PLATFORM_INSTANCE",
        ),
        "DP3 content features": dataset_urn(
            "trino",
            f"{iceberg_catalog}.{feature_schema}.feat_content_popularity",
            "DATAHUB_TRINO_PLATFORM_INSTANCE",
        ),
        "Streaming contract": dataset_urn(
            "kafka",
            "playback_events",
            "DATAHUB_KAFKA_PLATFORM_INSTANCE",
        ),
    }
    pipelines = {
        dag_id: make_data_flow_urn(
            "airflow",
            dag_id,
            required_environment("AIRFLOW_OPENLINEAGE_NAMESPACE"),
        )
        for dag_id in [
            "dp1_raw_to_bronze",
            "dp2_bronze_to_gold",
            "dp3_offline_features",
        ]
    }
    contracts = {
        "DP1": contract_urn("dp1-bronze-playback-events"),
        "DP2": contract_urn("dp2-serving-content-trending"),
        "DP3 user": contract_urn("dp3-user-engagement-features"),
        "DP3 content": contract_urn("dp3-content-popularity-features"),
        "Streaming": contract_urn("stream-playback-events-avro"),
    }

    checks: dict[str, object] = {
        "datasets": {},
        "pipelines": {},
        "contracts": {},
    }
    for label, urn in datasets.items():
        if not graph.exists(urn):
            raise RuntimeError(f"Dataset is missing: {label}: {urn}")
        required_aspects = {
            "ownership": graph.get_aspect(urn, OwnershipClass),
            "domains": graph.get_aspect(urn, DomainsClass),
            "tags": graph.get_aspect(urn, GlobalTagsClass),
        }
        missing = [name for name, aspect in required_aspects.items() if aspect is None]
        if missing:
            raise RuntimeError(f"Dataset {label} is missing aspects: {missing}")
        checks["datasets"][label] = "owner/domain/tags present"

    for dag_id, urn in pipelines.items():
        if not graph.exists(urn):
            raise RuntimeError(f"Pipeline is missing: {dag_id}: {urn}")
        checks["pipelines"][dag_id] = "runtime flow present"

    lineage_jobs = {
        "DP1": (
            "dp1_raw_to_bronze",
            "dp1_raw_to_bronze.ingest.raw_to_bronze",
            datasets["DP1 Bronze"],
        ),
        "DP2": (
            "dp2_bronze_to_gold",
            "dp2_bronze_to_gold.ingest.gold_and_serving",
            datasets["DP2 serving"],
        ),
        "DP3": (
            "dp3_offline_features",
            "dp3_offline_features.ingest.compute_offline_features",
            datasets["DP3 user features"],
        ),
    }
    for label, (flow_id, job_id, expected_output) in lineage_jobs.items():
        job_urn = make_data_job_urn(
            "airflow",
            flow_id,
            job_id,
            required_environment("AIRFLOW_OPENLINEAGE_NAMESPACE"),
        )
        if graph.get_aspect(job_urn, DataJobInfoClass) is None:
            raise RuntimeError(f"OpenLineage job is missing: {label}: {job_urn}")
        lineage = graph.get_aspect(job_urn, DataJobInputOutputClass)
        if lineage is None:
            raise RuntimeError(f"OpenLineage job has no dataset lineage: {label}")
        outputs = {
            edge.destinationUrn for edge in (lineage.outputDatasetEdges or [])
        }
        if expected_output not in outputs:
            raise RuntimeError(
                f"OpenLineage output is missing for {label}: {expected_output}"
            )
        checks["pipelines"][flow_id] = "runtime input/output lineage present"

    for label, urn in contracts.items():
        properties = graph.get_aspect(urn, DataContractPropertiesClass)
        if properties is None:
            raise RuntimeError(f"Data Contract is missing: {label}: {urn}")
        assertion_urns = [
            item.assertion
            for item in [*(properties.schema or []), *(properties.dataQuality or [])]
        ]
        if not assertion_urns:
            raise RuntimeError(f"Data Contract has no assertions: {label}")
        for assertion_urn in assertion_urns:
            if graph.get_aspect(assertion_urn, AssertionInfoClass) is None:
                raise RuntimeError(f"Assertion definition is missing: {assertion_urn}")
            latest = graph.get_latest_timeseries_value(
                assertion_urn,
                AssertionRunEventClass,
                {},
            )
            if latest is None or latest.result is None:
                raise RuntimeError(f"Assertion result is missing: {assertion_urn}")
            if latest.result.type != AssertionResultTypeClass.SUCCESS:
                raise RuntimeError(f"Assertion did not pass: {assertion_urn}")
        checks["contracts"][label] = f"{len(assertion_urns)} passing assertions"

    print(json.dumps(checks, indent=2))


if __name__ == "__main__":
    main()
