"""Build and submit the Kafka-to-PostgreSQL Flink execution graph.

Reads: Kafka raw bytes, Schema Registry contracts, and versioned job configuration.
Writes: PostgreSQL five-minute aggregates and a Kafka DLQ topic.
Runs: as the streaming component's main orchestration layer.
"""

from __future__ import annotations

from pyflink.common import Configuration
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import (
    CheckpointingMode,
    ExternalizedCheckpointCleanup,
    OutputTag,
    StreamExecutionEnvironment,
)
from pyflink.table import DataTypes, EnvironmentSettings, Schema, StreamTableEnvironment

from flink_jobs.config import FlinkJobConfig
from flink_jobs.contracts import ConfluentAvroCodec
from flink_jobs.functions import (
    DLQ_TYPE,
    PLAYBACK_TYPE,
    ContractValidationFunction,
    LateAndDuplicateFunction,
)
from flink_jobs.kafka_admin import KafkaAdminGateway
from flink_jobs.schema_registry import SchemaRegistryGateway
from flink_jobs.settings import RuntimeSettings


def submit_pipeline(config: FlinkJobConfig, settings: RuntimeSettings) -> None:
    """Register contracts, construct named operators, and submit one streaming job."""
    kafka_admin = KafkaAdminGateway(settings.kafka_bootstrap_servers)
    kafka_admin.ensure_topic(
        config.kafka.source_topic, config.kafka.source_partitions
    )
    kafka_admin.ensure_topic(config.kafka.dlq_topic, config.kafka.dlq_partitions)
    registry = SchemaRegistryGateway(settings.schema_registry_url)
    playback_schema, playback_schema_id = registry.register(
        f"{config.kafka.source_topic}-value", settings.playback_schema_path
    )
    dlq_schema, dlq_schema_id = registry.register(
        f"{config.kafka.dlq_topic}-value", settings.dlq_schema_path
    )

    environment, table_environment = _create_environments(config, settings)
    _create_source_table(table_environment, config, settings)
    _create_dlq_sink(table_environment, config, settings)
    _create_jdbc_sinks(table_environment, config, settings)

    raw_stream = table_environment.to_data_stream(
        table_environment.from_path("playback_events_source")
    )
    dlq_tag = OutputTag("invalid-playback-events", DLQ_TYPE)
    validated_stream = (
        raw_stream.process(
            ContractValidationFunction(
                ConfluentAvroCodec(playback_schema, playback_schema_id),
                ConfluentAvroCodec(dlq_schema, dlq_schema_id),
                config.kafka.source_topic,
                dlq_tag,
            ),
            output_type=PLAYBACK_TYPE,
        )
        .name("01 Validate Confluent Avro contract and route DLQ")
        .uid("contract-validation")
        .set_parallelism(config.processing.parallelism)
    )
    dlq_stream = validated_stream.get_side_output(dlq_tag)

    processed_stream = (
        validated_stream.key_by(lambda row: row[0])
        .process(
            LateAndDuplicateFunction(
                late_handling_enabled=config.processing.late_handling_enabled,
                dedup_enabled=config.processing.dedup_enabled,
                dedup_ttl_minutes=config.processing.dedup_ttl_minutes,
            ),
            output_type=PLAYBACK_TYPE,
        )
        .name("02 Apply late-event policy and event-id deduplication")
        .uid("late-and-dedup")
        .set_parallelism(config.processing.parallelism)
    )

    event_schema = (
        Schema.new_builder()
        .column("event_id", DataTypes.STRING())
        .column("user_id", DataTypes.STRING())
        .column("content_id", DataTypes.STRING())
        .column("session_id", DataTypes.STRING())
        .column("event_type", DataTypes.STRING())
        .column("position_seconds", DataTypes.INT())
        .column("watch_seconds", DataTypes.INT())
        .column("event_timestamp", DataTypes.BIGINT())
        .column("produced_timestamp", DataTypes.BIGINT())
        .column("payload_schema_version", DataTypes.INT())
        .column_by_expression("event_time", "TO_TIMESTAMP_LTZ(event_timestamp, 3)")
        .watermark("event_time", "SOURCE_WATERMARK()")
        .build()
    )
    table_environment.create_temporary_view(
        "validated_playback_events",
        table_environment.from_data_stream(processed_stream, event_schema),
    )
    dlq_schema_definition = (
        Schema.new_builder()
        .column("dlq_key", DataTypes.BYTES())
        .column("payload", DataTypes.BYTES())
        .build()
    )
    table_environment.create_temporary_view(
        "invalid_playback_events",
        table_environment.from_data_stream(dlq_stream, dlq_schema_definition),
    )

    statement_set = table_environment.create_statement_set()
    statement_set.add_insert_sql(
        _playback_window_insert(config.processing.window_minutes)
    )
    statement_set.add_insert_sql(
        _content_window_insert(config.processing.window_minutes)
    )
    statement_set.add_insert_sql(
        "INSERT INTO playback_events_dlq_sink SELECT dlq_key, payload "
        "FROM invalid_playback_events"
    )
    statement_set.execute()


def _create_environments(
    config: FlinkJobConfig, settings: RuntimeSettings
) -> tuple[StreamExecutionEnvironment, StreamTableEnvironment]:
    """Configure checkpointing, state, restart behavior, and Table API execution."""
    runtime = Configuration()
    runtime.set_string("pipeline.name", config.job_name)
    runtime.set_string("python.executable", settings.python_executable)
    runtime.set_string("python.client.executable", settings.python_executable)
    runtime.set_string("state.backend.type", config.state.backend)
    runtime.set_boolean(
        "execution.checkpointing.incremental", config.state.incremental
    )
    runtime.set_string("table.local-time-zone", "UTC")
    environment = StreamExecutionEnvironment.get_execution_environment(runtime)
    environment.set_parallelism(config.processing.parallelism)
    environment.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            config.restart_attempts, config.restart_delay_ms
        )
    )
    environment.enable_checkpointing(
        config.checkpoint.interval_ms, CheckpointingMode.EXACTLY_ONCE
    )
    checkpoint = environment.get_checkpoint_config()
    checkpoint.set_checkpoint_timeout(config.checkpoint.timeout_ms)
    checkpoint.set_min_pause_between_checkpoints(config.checkpoint.min_pause_ms)
    checkpoint.set_max_concurrent_checkpoints(config.checkpoint.max_concurrent)
    checkpoint.set_tolerable_checkpoint_failure_number(
        config.checkpoint.tolerable_failures
    )
    checkpoint.enable_unaligned_checkpoints(config.checkpoint.unaligned_enabled)
    checkpoint.enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    checkpoint.set_checkpoint_storage_dir(settings.checkpoint_uri(config.job_name))
    table_environment = StreamTableEnvironment.create(
        environment, environment_settings=EnvironmentSettings.in_streaming_mode()
    )
    table_environment.get_config().get_configuration().set_string(
        "pipeline.name", config.job_name
    )
    table_environment.get_config().get_configuration().set_string(
        "table.exec.source.idle-timeout",
        f"{config.processing.source_idle_timeout_seconds}s",
    )
    return environment, table_environment


def _create_source_table(
    table_environment: StreamTableEnvironment,
    config: FlinkJobConfig,
    settings: RuntimeSettings,
) -> None:
    """Create a raw Kafka table that preserves malformed value bytes and offsets."""
    table_environment.execute_sql(
        f"""
        CREATE TEMPORARY TABLE playback_events_source (
            kafka_key BYTES,
            payload BYTES,
            source_partition INT METADATA FROM 'partition' VIRTUAL,
            source_offset BIGINT METADATA FROM 'offset' VIRTUAL,
            source_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
            WATERMARK FOR source_timestamp AS source_timestamp
                - INTERVAL '{config.processing.watermark_seconds}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{_literal(config.kafka.source_topic)}',
            'properties.bootstrap.servers' = '{_literal(settings.kafka_bootstrap_servers)}',
            'properties.group.id' = '{_literal(config.kafka.consumer_group)}',
            'properties.enable.auto.commit' = 'false',
            'properties.isolation.level' = 'read_committed',
            'scan.startup.mode' = '{_literal(config.kafka.startup_mode)}',
            'key.format' = 'raw',
            'key.fields' = 'kafka_key',
            'value.format' = 'raw',
            'value.fields-include' = 'EXCEPT_KEY'
        )
        """
    )


def _create_dlq_sink(
    table_environment: StreamTableEnvironment,
    config: FlinkJobConfig,
    settings: RuntimeSettings,
) -> None:
    """Create a raw Kafka sink for pre-encoded Confluent Avro DLQ records."""
    table_environment.execute_sql(
        f"""
        CREATE TEMPORARY TABLE playback_events_dlq_sink (
            dlq_key BYTES,
            payload BYTES
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{_literal(config.kafka.dlq_topic)}',
            'properties.bootstrap.servers' = '{_literal(settings.kafka_bootstrap_servers)}',
            'key.format' = 'raw',
            'key.fields' = 'dlq_key',
            'value.format' = 'raw',
            'value.fields-include' = 'EXCEPT_KEY',
            'sink.delivery-guarantee' = 'at-least-once'
        )
        """
    )


def _create_jdbc_sinks(
    table_environment: StreamTableEnvironment,
    config: FlinkJobConfig,
    settings: RuntimeSettings,
) -> None:
    """Create two primary-key JDBC sinks that use PostgreSQL upsert semantics."""
    common_options = f"""
        'connector' = 'jdbc',
        'url' = '{_literal(settings.jdbc_url)}',
        'username' = '{_literal(settings.postgres_user)}',
        'password' = '{_literal(settings.postgres_password)}',
        'driver' = 'org.postgresql.Driver',
        'sink.buffer-flush.max-rows' = '{config.jdbc.buffer_flush_max_rows}',
        'sink.buffer-flush.interval' = '{config.jdbc.buffer_flush_interval_ms}ms',
        'sink.max-retries' = '{config.jdbc.max_retries}'
    """
    playback_table = (
        f"{settings.postgres_streaming_schema}.{config.outputs.playback_metrics_table}"
    )
    content_table = (
        f"{settings.postgres_streaming_schema}.{config.outputs.content_popularity_table}"
    )
    table_environment.execute_sql(
        f"""
        CREATE TEMPORARY TABLE playback_metrics_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            user_id STRING,
            event_count BIGINT,
            unique_session_count BIGINT,
            unique_content_count BIGINT,
            playback_started_count BIGINT,
            playback_completed_count BIGINT,
            completed_watch_seconds BIGINT,
            created_timestamp TIMESTAMP(3),
            PRIMARY KEY (window_start, user_id) NOT ENFORCED
        ) WITH (
            'table-name' = '{_literal(playback_table)}',
            {common_options}
        )
        """
    )
    table_environment.execute_sql(
        f"""
        CREATE TEMPORARY TABLE content_popularity_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            content_id STRING,
            event_count BIGINT,
            unique_user_count BIGINT,
            unique_session_count BIGINT,
            playback_started_count BIGINT,
            playback_completed_count BIGINT,
            completed_watch_seconds BIGINT,
            created_timestamp TIMESTAMP(3),
            PRIMARY KEY (window_start, content_id) NOT ENFORCED
        ) WITH (
            'table-name' = '{_literal(content_table)}',
            {common_options}
        )
        """
    )


def _playback_window_insert(window_minutes: int) -> str:
    """Return the five-minute per-user window aggregation required for proof."""
    return f"""
        INSERT INTO playback_metrics_sink
        SELECT
            CAST(window_start AS TIMESTAMP(3)),
            CAST(window_end AS TIMESTAMP(3)),
            user_id,
            COUNT(*),
            COUNT(DISTINCT session_id),
            COUNT(DISTINCT content_id),
            SUM(CASE WHEN event_type = 'playback_started' THEN 1 ELSE 0 END),
            SUM(CASE WHEN event_type = 'playback_completed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN event_type = 'playback_completed' THEN watch_seconds ELSE 0 END),
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
        FROM TABLE(
            TUMBLE(
                TABLE validated_playback_events,
                DESCRIPTOR(event_time),
                INTERVAL '{window_minutes}' MINUTE
            )
        )
        GROUP BY window_start, window_end, user_id
    """


def _content_window_insert(window_minutes: int) -> str:
    """Return the five-minute per-content popularity aggregation required for proof."""
    return f"""
        INSERT INTO content_popularity_sink
        SELECT
            CAST(window_start AS TIMESTAMP(3)),
            CAST(window_end AS TIMESTAMP(3)),
            content_id,
            COUNT(*),
            COUNT(DISTINCT user_id),
            COUNT(DISTINCT session_id),
            SUM(CASE WHEN event_type = 'playback_started' THEN 1 ELSE 0 END),
            SUM(CASE WHEN event_type = 'playback_completed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN event_type = 'playback_completed' THEN watch_seconds ELSE 0 END),
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
        FROM TABLE(
            TUMBLE(
                TABLE validated_playback_events,
                DESCRIPTOR(event_time),
                INTERVAL '{window_minutes}' MINUTE
            )
        )
        GROUP BY window_start, window_end, content_id
    """


def _literal(value: str) -> str:
    """Escape one string for a Flink SQL literal."""
    return value.replace("'", "''")
