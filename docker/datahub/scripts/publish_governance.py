"""Publish CineFlux ownership, domains, tags, contracts, and assertion run results."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

from datahub.emitter.mce_builder import (
    make_assertion_urn,
    make_data_flow_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionTypeClass,
    CorpGroupInfoClass,
    CustomAssertionInfoClass,
    DataContractPropertiesClass,
    DataFlowInfoClass,
    DataQualityContractClass,
    DomainPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaContractClass,
    TagAssociationClass,
    TagPropertiesClass,
)


@dataclass(frozen=True)
class ContractAssertion:
    """Describe one deterministic DataHub assertion attached to a data contract."""

    assertion_id: str
    dataset_urn: str
    assertion_type: str
    description: str
    success: bool
    native_results: dict[str, str]

    @property
    def urn(self) -> str:
        """Return the deterministic assertion URN."""
        return make_assertion_urn(self.assertion_id)


def required_environment(name: str) -> str:
    """Return one required environment variable or fail with a clear message."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def trino_dataset(name: str) -> str:
    """Build a Trino dataset URN aligned with ingestion and OpenLineage metadata."""
    return make_dataset_urn_with_platform_instance(
        "trino",
        name,
        required_environment("DATAHUB_TRINO_PLATFORM_INSTANCE"),
        required_environment("DATAHUB_ENV"),
    )


def kafka_dataset(name: str) -> str:
    """Build a Kafka dataset URN aligned with Schema Registry ingestion."""
    return make_dataset_urn_with_platform_instance(
        "kafka",
        name,
        required_environment("DATAHUB_KAFKA_PLATFORM_INSTANCE"),
        required_environment("DATAHUB_ENV"),
    )


def postgres_dataset(name: str) -> str:
    """Build a PostgreSQL dataset URN aligned with ingestion and OpenLineage metadata."""
    return make_dataset_urn_with_platform_instance(
        "postgres",
        name,
        required_environment("DATAHUB_POSTGRES_PLATFORM_INSTANCE"),
        required_environment("DATAHUB_ENV"),
    )


def emit_aspect(emitter: DatahubRestEmitter, urn: str, aspect: object) -> None:
    """Emit one idempotent aspect proposal."""
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=aspect,
        )
    )


def publish_foundation(emitter: DatahubRestEmitter) -> tuple[str, str, dict[str, str]]:
    """Create the shared owner group, domain, and controlled tag vocabulary."""
    owner_id = required_environment("DATAHUB_OWNER_GROUP")
    owner_urn = f"urn:li:corpGroup:{owner_id}"
    emit_aspect(
        emitter,
        owner_urn,
        CorpGroupInfoClass(
            admins=[],
            members=[],
            groups=[],
            displayName="CineFlux Data Platform",
            description="Technical owner of the CineFlux data platform.",
        ),
    )

    domain_urn = make_domain_urn(required_environment("DATAHUB_DOMAIN_ID"))
    emit_aspect(
        emitter,
        domain_urn,
        DomainPropertiesClass(
            name=required_environment("DATAHUB_DOMAIN_NAME"),
            description="Batch, streaming, lakehouse, and serving data assets for CineFlux.",
        ),
    )

    tag_descriptions = {
        "batch": "Batch-generated or batch-processed data.",
        "streaming": "Streaming event or event-time aggregate data.",
        "bronze": "Immutable raw lakehouse data.",
        "silver": "Validated and normalized lakehouse data.",
        "gold": "Business-ready analytical data.",
        "feature": "Point-in-time offline feature data.",
        "serving": "PostgreSQL data optimized for downstream reads.",
        "data-contract": "Dataset governed by an explicit data contract.",
    }
    tag_urns = {name: make_tag_urn(name) for name in tag_descriptions}
    for name, description in tag_descriptions.items():
        emit_aspect(
            emitter,
            tag_urns[name],
            TagPropertiesClass(name=name, description=description),
        )
    return owner_urn, domain_urn, tag_urns


def enrich_entity(
    emitter: DatahubRestEmitter,
    urn: str,
    owner_urn: str,
    domain_urn: str,
    tag_urns: dict[str, str],
    tags: list[str],
) -> None:
    """Apply shared ownership, domain, and tags to one entity."""
    emit_aspect(
        emitter,
        urn,
        OwnershipClass(
            owners=[
                OwnerClass(
                    owner=owner_urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ]
        ),
    )
    emit_aspect(emitter, urn, DomainsClass(domains=[domain_urn]))
    emit_aspect(
        emitter,
        urn,
        GlobalTagsClass(
            tags=[TagAssociationClass(tag=tag_urns[tag]) for tag in tags]
        ),
    )


def dbt_test_summary(manifest_path: Path, run_results_path: Path, models: set[str]) -> dict[str, str]:
    """Summarize dbt test outcomes that depend on the selected models."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_results = json.loads(run_results_path.read_text(encoding="utf-8"))
    selected_node_ids = {
        node_id
        for node_id, node in manifest["nodes"].items()
        if node.get("resource_type") == "model" and node.get("name") in models
    }
    test_ids = {
        node_id
        for node_id, node in manifest["nodes"].items()
        if node.get("resource_type") == "test"
        and selected_node_ids.intersection(node.get("depends_on", {}).get("nodes", []))
    }
    statuses = [
        result["status"]
        for result in run_results["results"]
        if result["unique_id"] in test_ids
    ]
    if not statuses:
        raise RuntimeError(f"No dbt test results found for models: {sorted(models)}")
    failures = sum(status not in {"pass", "warn"} for status in statuses)
    return {
        "tests": str(len(statuses)),
        "passed": str(len(statuses) - failures),
        "failed": str(failures),
    }


def dbt_schema_result(
    catalog_path: Path,
    model_name: str,
    schema_name: str,
    required_columns: set[str],
) -> tuple[bool, dict[str, str]]:
    """Validate required model columns against the generated dbt catalog."""
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    matching_nodes = [
        node
        for node in catalog["nodes"].values()
        if node.get("metadata", {}).get("name") == model_name
        and node.get("metadata", {}).get("schema") == schema_name
    ]
    if len(matching_nodes) != 1:
        raise RuntimeError(
            f"Expected one dbt catalog node for {schema_name}.{model_name}, "
            f"found {len(matching_nodes)}"
        )
    actual_columns = set(matching_nodes[0].get("columns", {}))
    missing_columns = required_columns - actual_columns
    return not missing_columns, {
        "model": f"{schema_name}.{model_name}",
        "required_columns": ",".join(sorted(required_columns)),
        "missing_columns": ",".join(sorted(missing_columns)),
    }


def schema_registry_result(
    subject: str,
    required_fields: set[str],
) -> tuple[bool, dict[str, str]]:
    """Validate the latest registered Avro schema for one subject."""
    registry_url = required_environment("SCHEMA_REGISTRY_URL").rstrip("/")
    endpoint = f"{registry_url}/subjects/{quote(subject, safe='')}/versions/latest"
    with urlopen(endpoint, timeout=10) as response:
        payload = json.load(response)
    schema = json.loads(payload["schema"])
    actual_fields = {field["name"] for field in schema.get("fields", [])}
    missing_fields = required_fields - actual_fields
    success = schema.get("type") == "record" and not missing_fields
    return success, {
        "subject": str(payload["subject"]),
        "version": str(payload["version"]),
        "schema_id": str(payload["id"]),
        "schema_name": str(schema.get("name", "")),
        "missing_fields": ",".join(sorted(missing_fields)),
    }


def publish_assertion(emitter: DatahubRestEmitter, assertion: ContractAssertion) -> None:
    """Publish one assertion definition and its latest completed run result."""
    now_ms = int(time.time() * 1000)
    emit_aspect(
        emitter,
        assertion.urn,
        AssertionInfoClass(
            type=AssertionTypeClass.CUSTOM,
            customAssertion=CustomAssertionInfoClass(
                type=assertion.assertion_type,
                entity=assertion.dataset_urn,
                logic=assertion.description,
            ),
            source=AssertionSourceClass(type=AssertionSourceTypeClass.EXTERNAL),
            description=assertion.description,
        ),
    )
    emit_aspect(
        emitter,
        assertion.urn,
        AssertionRunEventClass(
            timestampMillis=now_ms,
            runId=f"governance-{now_ms}",
            asserteeUrn=assertion.dataset_urn,
            status=AssertionRunStatusClass.COMPLETE,
            assertionUrn=assertion.urn,
            result=AssertionResultClass(
                type=(
                    AssertionResultTypeClass.SUCCESS
                    if assertion.success
                    else AssertionResultTypeClass.FAILURE
                ),
                nativeResults=assertion.native_results,
            ),
        ),
    )


def contract_urn(contract_id: str) -> str:
    """Build a stable Data Contract URN from a repository-owned identifier."""
    digest = hashlib.sha256(contract_id.encode("utf-8")).hexdigest()[:32]
    return f"urn:li:dataContract:{digest}"


def publish_contract(
    emitter: DatahubRestEmitter,
    contract_id: str,
    dataset_urn: str,
    schema_assertions: list[ContractAssertion],
    quality_assertions: list[ContractAssertion],
) -> None:
    """Publish a dataset contract and all assertion definitions and results it references."""
    for assertion in [*schema_assertions, *quality_assertions]:
        publish_assertion(emitter, assertion)
    emit_aspect(
        emitter,
        contract_urn(contract_id),
        DataContractPropertiesClass(
            entity=dataset_urn,
            schema=[SchemaContractClass(assertion=item.urn) for item in schema_assertions],
            dataQuality=[
                DataQualityContractClass(assertion=item.urn)
                for item in quality_assertions
            ],
        ),
    )


def main() -> None:
    """Publish governance metadata derived from GX and dbt validation artifacts."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--gx-results", required=True, type=Path)
    args = parser.parse_args()

    gx_summary = json.loads(args.gx_results.read_text(encoding="utf-8"))
    artifacts = Path("/opt/cineflux-governance/artifacts/dbt")
    catalog_path = artifacts / "catalog.json"
    dp2_tests = dbt_test_summary(
        artifacts / "manifest.json",
        artifacts / "run_results.json",
        {"mart_content_trending", "publish_mart_content_trending"},
    )
    dp3_tests = dbt_test_summary(
        artifacts / "manifest.json",
        artifacts / "run_results.json",
        {"feat_user_engagement", "feat_content_popularity"},
    )
    dp2_schema_success, dp2_schema_results = dbt_schema_result(
        catalog_path,
        "mart_content_trending",
        required_environment("POSTGRES_SERVING_SCHEMA"),
        {
            "window_start",
            "window_end",
            "content_id",
            "playback_count",
            "unique_viewer_count",
            "completed_session_count",
            "total_watch_seconds",
            "popularity_score",
            "popularity_rank",
            "created_timestamp",
        },
    )
    feature_schema_results = {
        model_name: dbt_schema_result(
            catalog_path,
            model_name,
            required_environment("DBT_FEATURE_SCHEMA"),
            {"event_timestamp", "created"},
        )
        for model_name in ["feat_user_engagement", "feat_content_popularity"]
    }
    streaming_schema_success, streaming_schema_results = schema_registry_result(
        "playback_events-value",
        {
            "event_id",
            "user_id",
            "content_id",
            "session_id",
            "event_type",
            "event_timestamp",
            "produced_timestamp",
            "payload_schema_version",
        },
    )

    emitter = DatahubRestEmitter(gms_server=required_environment("DATAHUB_GMS_URL"))
    emitter.test_connection()
    owner_urn, domain_urn, tag_urns = publish_foundation(emitter)

    iceberg_catalog = required_environment("DBT_ICEBERG_CATALOG")
    bronze_schema = required_environment("AIRFLOW_BRONZE_SCHEMA")
    feature_schema = required_environment("DBT_FEATURE_SCHEMA")
    postgres_database = required_environment("CINEFLUX_POSTGRES_DB")
    serving_schema = required_environment("POSTGRES_SERVING_SCHEMA")
    datasets = {
        "dp1": trino_dataset(
            f"{iceberg_catalog}.{bronze_schema}.raw_playback_events"
        ),
        "dp2": postgres_dataset(
            f"{postgres_database}.{serving_schema}.mart_content_trending"
        ),
        "dp3_user": trino_dataset(
            f"{iceberg_catalog}.{feature_schema}.feat_user_engagement"
        ),
        "dp3_content": trino_dataset(
            f"{iceberg_catalog}.{feature_schema}.feat_content_popularity"
        ),
        "stream": kafka_dataset("playback_events"),
    }
    for key, tags in {
        "dp1": ["batch", "bronze", "data-contract"],
        "dp2": ["batch", "serving", "data-contract"],
        "dp3_user": ["batch", "feature", "data-contract"],
        "dp3_content": ["batch", "feature", "data-contract"],
        "stream": ["streaming", "data-contract"],
    }.items():
        enrich_entity(
            emitter,
            datasets[key],
            owner_urn,
            domain_urn,
            tag_urns,
            tags,
        )

    pipeline_descriptions = {
        "dp1_raw_to_bronze": "Landing files to validated Bronze datasets.",
        "dp2_bronze_to_gold": "Bronze to Silver, Gold, and PostgreSQL serving.",
        "dp3_offline_features": "Gold models to validated offline feature tables.",
    }
    for dag_id, description in pipeline_descriptions.items():
        flow_urn = make_data_flow_urn(
            "airflow",
            dag_id,
            required_environment("AIRFLOW_OPENLINEAGE_NAMESPACE"),
        )
        emit_aspect(
            emitter,
            flow_urn,
            DataFlowInfoClass(
                name=dag_id,
                description=description,
                project="CineFlux Data Platform",
            ),
        )
        enrich_entity(
            emitter,
            flow_urn,
            owner_urn,
            domain_urn,
            tag_urns,
            ["batch"],
        )

    publish_contract(
        emitter,
        "dp1-bronze-playback-events",
        datasets["dp1"],
        [
            ContractAssertion(
                "dp1-bronze-playback-schema",
                datasets["dp1"],
                "SCHEMA",
                "Required playback event columns are present in Bronze.",
                bool(gx_summary["success"]),
                {"suite": str(gx_summary["suite"])},
            )
        ],
        [
            ContractAssertion(
                "dp1-bronze-playback-quality",
                datasets["dp1"],
                "DATA_QUALITY",
                "The latest Great Expectations Bronze suite passes.",
                bool(gx_summary["success"]),
                {
                    key: str(value)
                    for key, value in gx_summary["statistics"].items()
                },
            )
        ],
    )
    publish_contract(
        emitter,
        "dp2-serving-content-trending",
        datasets["dp2"],
        [
            ContractAssertion(
                "dp2-serving-trending-schema",
                datasets["dp2"],
                "SCHEMA",
                "The serving mart exposes the contracted trending grain and metrics.",
                dp2_schema_success,
                dp2_schema_results,
            )
        ],
        [
            ContractAssertion(
                "dp2-serving-trending-quality",
                datasets["dp2"],
                "DATA_QUALITY",
                "The latest dbt tests for Gold and serving models pass.",
                dp2_tests["failed"] == "0",
                dp2_tests,
            )
        ],
    )
    for key, model_name, contract_id, assertion_prefix in [
        (
            "dp3_user",
            "feat_user_engagement",
            "dp3-user-engagement-features",
            "dp3-user-feature",
        ),
        (
            "dp3_content",
            "feat_content_popularity",
            "dp3-content-popularity-features",
            "dp3-content-feature",
        ),
    ]:
        schema_success, schema_results = feature_schema_results[model_name]
        publish_contract(
            emitter,
            contract_id,
            datasets[key],
            [
                ContractAssertion(
                    f"{assertion_prefix}-schema",
                    datasets[key],
                    "SCHEMA",
                    "The feature table contains event_timestamp and created.",
                    schema_success,
                    schema_results,
                )
            ],
            [
                ContractAssertion(
                    f"{assertion_prefix}-quality",
                    datasets[key],
                    "DATA_QUALITY",
                    "The latest dbt feature contract tests pass.",
                    dp3_tests["failed"] == "0",
                    dp3_tests,
                )
            ],
        )
    publish_contract(
        emitter,
        "stream-playback-events-avro",
        datasets["stream"],
        [
            ContractAssertion(
                "stream-playback-events-avro-schema",
                datasets["stream"],
                "SCHEMA",
                "Schema Registry is authoritative for the playback_events Avro contract.",
                streaming_schema_success,
                streaming_schema_results,
            )
        ],
        [],
    )
    emitter.close()
    print(
        json.dumps(
            {
                "datasets_enriched": len(datasets),
                "pipelines_enriched": len(pipeline_descriptions),
                "contracts_published": 5,
                "dp1_gx_success": bool(gx_summary["success"]),
                "dp2_dbt_tests": dp2_tests,
                "dp3_dbt_tests": dp3_tests,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
