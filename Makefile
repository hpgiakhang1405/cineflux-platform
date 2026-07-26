SHELL := /bin/bash

-include .env

COMPOSE := docker compose
PROFILES := messaging storage processing spark-processing flink-processing dbt-processing orchestration analytics governance generator-batch generator-stream
GENERATOR_CONFIG ?= smoke
SPARK_CONFIG ?=
FLINK_CONFIG ?=
FLINK_JOB_ID ?=

.DEFAULT_GOAL := help

.PHONY: help env check-local-env check-profile check-service check-delivery check-execution-id check-pipeline-run-id check-spark-config check-flink-config check-flink-job-id check-dag-id config up ps logs stop \
	generator-build generator-build-baseline generator-bootstrap generator-batch \
	generator-stream spark-build spark-build-baseline spark-up spark-dp1 spark-dp2 \
	flink-build flink-build-baseline flink-up flink-migrate flink-submit flink-cancel flink-reset-data \
	airflow-build airflow-build-baseline airflow-up airflow-import-check airflow-connections airflow-variables airflow-trigger \
	docker-image-benchmark \
	dbt-build dbt-up dbt-debug dbt-smoke dbt-migrate dbt-run dbt-test dbt-docs dbt-docs-serve \
	storage-prepare storage-lakehouse-files storage-lakehouse-benchmark storage-compact \
	storage-index-reset storage-index-create storage-index-benchmark

help:
	@printf '%s\n' \
		'Usage:' \
		'  make env' \
		'  make config PROFILE=<profile>' \
		'  make up PROFILE=<profile>' \
		'  make ps PROFILE=<profile>' \
		'  make logs SERVICE=<service>' \
		'  make stop PROFILE=<profile>' \
		'  make generator-build' \
		'  make generator-build-baseline' \
		'  make generator-bootstrap GENERATOR_CONFIG=smoke|demo' \
		'  make generator-batch GENERATOR_CONFIG=smoke|demo DELIVERY=delivery_001' \
		'  make generator-stream GENERATOR_CONFIG=smoke|demo|flink_demo EXECUTION_ID=<id>' \
		'  make spark-build' \
		'  make spark-build-baseline' \
		'  make spark-up' \
		'  make spark-dp1 SPARK_CONFIG=spark_dp1_smoke PIPELINE_RUN_ID=<id>' \
		'  make spark-dp2 SPARK_CONFIG=spark_dp2_smoke PIPELINE_RUN_ID=<id>' \
		'  make flink-build' \
		'  make flink-build-baseline' \
		'  make docker-image-benchmark' \
		'  make flink-up' \
		'  make flink-migrate' \
		'  make flink-submit FLINK_CONFIG=flink_smoke' \
		'  make flink-cancel FLINK_JOB_ID=<job-id>' \
		'  make flink-reset-data FLINK_CONFIG=flink_smoke' \
		'  make airflow-build' \
		'  make airflow-build-baseline' \
		'  make airflow-up' \
		'  make airflow-import-check' \
		'  make airflow-connections' \
		'  make airflow-variables' \
		'  make airflow-trigger DAG_ID=dp1_raw_to_bronze' \
		'  make dbt-build' \
		'  make dbt-up' \
		'  make dbt-debug' \
		'  make dbt-smoke' \
		'  make dbt-migrate' \
		'  make dbt-run' \
		'  make dbt-test' \
		'  make dbt-docs' \
		'  make dbt-docs-serve' \
		'  make storage-prepare PIPELINE_RUN_ID=<id>' \
		'  make storage-lakehouse-files' \
		'  make storage-lakehouse-benchmark' \
		'  make storage-compact PIPELINE_RUN_ID=<id>' \
		'  make storage-index-reset' \
		'  make storage-index-create' \
		'  make storage-index-benchmark' \
		'' \
		'Profiles: $(PROFILES)'

env:
	@bash docker/scripts/create_local_env.sh

check-local-env:
	@if [[ ! -f .env ]]; then \
		echo '.env is required. Run make env first.' >&2; \
		exit 1; \
	fi

check-profile:
	@if [[ -z "$(PROFILE)" ]]; then \
		echo 'PROFILE is required. Run make help for usage.' >&2; \
		exit 1; \
	fi
	@if [[ ! " $(PROFILES) " =~ " $(PROFILE) " ]]; then \
		echo "Unsupported PROFILE='$(PROFILE)'. Expected one of: $(PROFILES)" >&2; \
		exit 1; \
	fi

check-service:
	@if [[ -z "$(SERVICE)" ]]; then \
		echo 'SERVICE is required. Run make help for usage.' >&2; \
		exit 1; \
	fi

check-delivery:
	@if [[ -z "$(DELIVERY)" ]]; then \
		echo 'DELIVERY is required. Run make help for usage.' >&2; \
		exit 1; \
	fi

check-execution-id:
	@if [[ -z "$(EXECUTION_ID)" ]]; then \
		echo 'EXECUTION_ID is required. Run make help for usage.' >&2; \
		exit 1; \
	fi

check-pipeline-run-id:
	@if [[ -z "$(PIPELINE_RUN_ID)" ]]; then \
		echo 'PIPELINE_RUN_ID is required. Run make help for usage.' >&2; \
		exit 1; \
	fi

check-spark-config:
	@if [[ -z "$(SPARK_CONFIG)" ]]; then \
		echo 'SPARK_CONFIG is required. Run make help for usage.' >&2; \
		exit 1; \
	fi
	@if [[ ! -f "data_platform/spark_jobs/config/$(SPARK_CONFIG).yaml" ]]; then \
		echo "Unknown Spark config: $(SPARK_CONFIG)" >&2; \
		exit 1; \
	fi

check-flink-config:
	@if [[ -z "$(FLINK_CONFIG)" ]]; then \
		echo 'FLINK_CONFIG is required. Run make help for usage.' >&2; \
		exit 1; \
	fi
	@if [[ ! -f "data_platform/flink_jobs/config/$(FLINK_CONFIG).yaml" ]]; then \
		echo "Unknown Flink config: $(FLINK_CONFIG)" >&2; \
		exit 1; \
	fi

check-flink-job-id:
	@if [[ -z "$(FLINK_JOB_ID)" ]]; then \
		echo 'FLINK_JOB_ID is required. Run make help for usage.' >&2; \
		exit 1; \
	fi

check-dag-id:
	@if [[ -z "$(DAG_ID)" ]]; then \
		echo 'DAG_ID is required. Run make help for usage.' >&2; \
		exit 1; \
	fi

config: check-profile
	@$(COMPOSE) --profile "$(PROFILE)" config --quiet
	@echo "Compose configuration is valid for profile '$(PROFILE)'."

up: check-profile
	@$(COMPOSE) --profile "$(PROFILE)" up -d

ps: check-profile
	@$(COMPOSE) --profile "$(PROFILE)" ps

logs: check-service
	@$(COMPOSE) --profile '*' logs --tail=200 "$(SERVICE)"

stop: check-profile
	@$(COMPOSE) --profile "$(PROFILE)" stop

generator-build:
	@$(COMPOSE) --profile generator-batch build batch-data-generator

generator-build-baseline: check-local-env
	@docker build \
		--build-arg UV_IMAGE="$(UV_IMAGE)" \
		--build-arg PYTHON_BASELINE_IMAGE="$(PYTHON_BASELINE_IMAGE)" \
		-f data_platform/generator/Dockerfile.baseline \
		-t "$(DATA_GENERATOR_BASELINE_IMAGE)" .

generator-bootstrap:
	@$(COMPOSE) --profile generator-batch run --rm batch-data-generator \
		bootstrap --config "/app/config/batch_$(GENERATOR_CONFIG).yaml"

generator-batch: check-delivery
	@$(COMPOSE) --profile generator-batch run --rm batch-data-generator \
		recurring_batch --config "/app/config/batch_$(GENERATOR_CONFIG).yaml" \
		--delivery-id "$(DELIVERY)"

generator-stream: check-execution-id
	@$(COMPOSE) --profile generator-stream run --rm stream-data-generator \
		stream --config "/app/config/stream_$(GENERATOR_CONFIG).yaml" \
		--execution-id "$(EXECUTION_ID)"

spark-build:
	@$(COMPOSE) --profile spark-processing build spark-jobs

spark-build-baseline: check-local-env
	@docker build \
		--build-arg SPARK_IMAGE="$(SPARK_IMAGE)" \
		--build-arg UV_IMAGE="$(UV_IMAGE)" \
		--build-arg ICEBERG_VERSION="$(ICEBERG_VERSION)" \
		--build-arg HADOOP_AWS_VERSION="$(HADOOP_AWS_VERSION)" \
		--build-arg AWS_SDK_BUNDLE_VERSION="$(AWS_SDK_BUNDLE_VERSION)" \
		-f data_platform/spark_jobs/Dockerfile.baseline \
		-t "$(SPARK_JOBS_BASELINE_IMAGE)" .

spark-up:
	@$(COMPOSE) --profile spark-processing up -d \
		postgres minio minio-init hive-metastore trino \
		spark-events-init spark-master spark-worker spark-history-server

spark-dp1: check-spark-config check-pipeline-run-id
	@$(COMPOSE) --profile spark-processing run --rm --no-deps --use-aliases spark-jobs \
		dp1 --config "/app/config/$(SPARK_CONFIG).yaml" \
		--run-id "$(PIPELINE_RUN_ID)"

spark-dp2: check-spark-config check-pipeline-run-id
	@$(COMPOSE) --profile spark-processing run --rm --no-deps --use-aliases spark-jobs \
		dp2 --config "/app/config/$(SPARK_CONFIG).yaml" \
		--run-id "$(PIPELINE_RUN_ID)"

storage-compact: check-pipeline-run-id
	@$(COMPOSE) --profile spark-processing run --rm --no-deps --use-aliases spark-jobs \
		compact --config "/app/config/storage_compaction.yaml" \
		--run-id "$(PIPELINE_RUN_ID)"

storage-prepare: check-pipeline-run-id
	@$(COMPOSE) --profile spark-processing exec -T trino trino \
		--execute "$$(< schemas/iceberg/benchmarks/reset_bronze_tables.sql)"
	@$(COMPOSE) --profile spark-processing run --rm --no-deps --use-aliases spark-jobs \
		dp1 --config "/app/config/storage_ingestion.yaml" \
		--run-id "$(PIPELINE_RUN_ID)"

storage-lakehouse-files:
	@$(COMPOSE) --profile spark-processing exec -T trino trino --output-format ALIGNED \
		--execute "$$(< schemas/iceberg/benchmarks/raw_playback_events_files.sql)"

storage-lakehouse-benchmark:
	@$(COMPOSE) --profile spark-processing exec -T trino trino --output-format ALIGNED \
		--execute "$$(< schemas/iceberg/benchmarks/raw_playback_events_scan.sql)"

flink-build:
	@$(COMPOSE) --profile flink-processing build flink-jobmanager

flink-build-baseline: check-local-env
	@docker build \
		--build-arg FLINK_BASE_IMAGE="$(FLINK_IMAGE)" \
		--build-arg UV_IMAGE="$(UV_IMAGE)" \
		--build-arg PYTHON_VERSION="$(FLINK_PYTHON_VERSION)" \
		--build-arg FLINK_VERSION="$(FLINK_VERSION)" \
		--build-arg FLINK_KAFKA_CONNECTOR_VERSION="$(FLINK_KAFKA_CONNECTOR_VERSION)" \
		--build-arg FLINK_JDBC_CONNECTOR_VERSION="$(FLINK_JDBC_CONNECTOR_VERSION)" \
		--build-arg POSTGRES_JDBC_VERSION="$(POSTGRES_JDBC_VERSION)" \
		--build-arg ROCKSDB_JAR_SHA1="$(ROCKSDB_JAR_SHA1)" \
		--build-arg KAFKA_CONNECTOR_JAR_SHA1="$(KAFKA_CONNECTOR_JAR_SHA1)" \
		--build-arg JDBC_CONNECTOR_JAR_SHA1="$(JDBC_CONNECTOR_JAR_SHA1)" \
		--build-arg POSTGRES_JDBC_JAR_SHA1="$(POSTGRES_JDBC_JAR_SHA1)" \
		-f data_platform/flink_jobs/Dockerfile.baseline \
		-t "$(FLINK_JOBS_BASELINE_IMAGE)" .

docker-image-benchmark: check-local-env
	@measure() { \
		component="$$1"; \
		baseline_bytes="$$(docker image inspect --format '{{.Size}}' "$$2")"; \
		optimized_bytes="$$(docker image inspect --format '{{.Size}}' "$$3")"; \
		awk -v component="$$component" \
			-v baseline_bytes="$$baseline_bytes" \
			-v optimized_bytes="$$optimized_bytes" \
			'BEGIN { \
				printf "%-16s %14.1f %14.1f %11.1f%%\n", component, \
					baseline_bytes / 1048576, optimized_bytes / 1048576, \
					(baseline_bytes - optimized_bytes) * 100 / baseline_bytes \
			}'; \
	}; \
	printf '%-16s %14s %14s %12s\n' 'Component' 'Baseline MiB' 'Optimized MiB' 'Reduction'; \
	measure 'Data Generator' '$(DATA_GENERATOR_BASELINE_IMAGE)' '$(DATA_GENERATOR_IMAGE)'; \
	measure 'Spark Jobs' '$(SPARK_JOBS_BASELINE_IMAGE)' '$(SPARK_JOBS_IMAGE)'; \
	measure 'Flink Jobs' '$(FLINK_JOBS_BASELINE_IMAGE)' '$(FLINK_JOBS_IMAGE)'; \
	measure 'Airflow' '$(AIRFLOW_BASELINE_IMAGE)' '$(AIRFLOW_IMAGE)'

airflow-build:
	@$(COMPOSE) --profile orchestration build airflow-webserver

airflow-build-baseline: check-local-env
	@docker build \
		--build-arg AIRFLOW_BASE_IMAGE="$(AIRFLOW_BASE_IMAGE)" \
		--build-arg SPARK_JOBS_IMAGE="$(SPARK_JOBS_IMAGE)" \
		--build-arg AIRFLOW_VERSION="$(AIRFLOW_VERSION)" \
		--build-arg AIRFLOW_PYTHON_VERSION="$(AIRFLOW_PYTHON_VERSION)" \
		-f data_platform/airflow/Dockerfile.baseline \
		-t "$(AIRFLOW_BASELINE_IMAGE)" .

airflow-up:
	@$(COMPOSE) --profile orchestration up -d

airflow-import-check:
	@$(COMPOSE) --profile orchestration exec -T airflow-scheduler \
		airflow dags list-import-errors

airflow-connections:
	@$(COMPOSE) --profile orchestration exec -T airflow-scheduler \
		python /opt/airflow/scripts/list_connections.py

airflow-variables:
	@$(COMPOSE) --profile orchestration exec -T airflow-scheduler \
		airflow variables list --output table

airflow-trigger: check-dag-id
	@$(COMPOSE) --profile orchestration exec -T airflow-scheduler \
		airflow dags trigger "$(DAG_ID)"

flink-up:
	@$(COMPOSE) --profile flink-processing up -d \
		postgres minio minio-init kafka schema-registry kafka-ui \
		flink-jobmanager flink-taskmanager

flink-migrate:
	@$(COMPOSE) --profile flink-processing exec -T postgres \
		psql --username "$(POSTGRES_USER)" --dbname "$(CINEFLUX_POSTGRES_DB)" \
		--set=streaming_schema="$(POSTGRES_STREAMING_SCHEMA)" \
		--set=ON_ERROR_STOP=1 \
		--file=/platform-migrations/001_streaming_tables.sql

flink-submit: check-flink-config
	@$(COMPOSE) --profile flink-processing exec -T flink-jobmanager \
		flink run --detached --python /app/src/flink_jobs/cli.py \
		--config "/app/config/$(FLINK_CONFIG).yaml"

flink-cancel: check-flink-job-id
	@$(COMPOSE) --profile flink-processing exec -T flink-jobmanager \
		flink cancel "$(FLINK_JOB_ID)"

flink-reset-data: check-flink-config
	@for attempt in {1..10}; do \
		if ! curl --fail --silent "http://localhost:$(FLINK_UI_PORT)/jobs/overview" \
			| grep -Eq '"state":"(CREATED|INITIALIZING|RUNNING|FAILING|RESTARTING|CANCELLING)"'; then \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo 'Cancel active Flink jobs before resetting benchmark data.' >&2; \
	exit 1
	@source_topic="$$(sed -n 's/^  source_topic: //p' "data_platform/flink_jobs/config/$(FLINK_CONFIG).yaml" | head -n 1)"; \
	consumer_group="$$(sed -n 's/^  consumer_group: //p' "data_platform/flink_jobs/config/$(FLINK_CONFIG).yaml" | head -n 1)"; \
	dlq_topic="$$(sed -n 's/^  dlq_topic: //p' "data_platform/flink_jobs/config/$(FLINK_CONFIG).yaml" | head -n 1)"; \
	$(COMPOSE) --profile flink-processing exec -T kafka \
		kafka-topics --bootstrap-server "$(KAFKA_BOOTSTRAP_SERVERS)" --delete --if-exists --topic "$$source_topic"; \
	$(COMPOSE) --profile flink-processing exec -T kafka \
		kafka-topics --bootstrap-server "$(KAFKA_BOOTSTRAP_SERVERS)" --delete --if-exists --topic "$$dlq_topic"; \
	$(COMPOSE) --profile flink-processing exec -T kafka \
		kafka-consumer-groups --bootstrap-server "$(KAFKA_BOOTSTRAP_SERVERS)" --delete \
		--group "$$consumer_group" >/dev/null 2>&1 || true; \
	$(COMPOSE) --profile flink-processing exec -T postgres \
		psql --username "$(POSTGRES_USER)" --dbname "$(CINEFLUX_POSTGRES_DB)" \
		--set=ON_ERROR_STOP=1 \
		--command "TRUNCATE $(POSTGRES_STREAMING_SCHEMA).playback_metrics_5m, $(POSTGRES_STREAMING_SCHEMA).content_popularity_5m;"

dbt-build:
	@$(COMPOSE) --profile dbt-processing build dbt-jobs

dbt-up:
	@$(COMPOSE) --profile dbt-processing up -d \
		postgres minio minio-init hive-metastore trino

dbt-debug:
	@$(COMPOSE) --profile dbt-processing run --rm dbt-jobs debug

dbt-smoke:
	@$(COMPOSE) --profile dbt-processing run --rm dbt-jobs run-operation \
		iceberg_write_smoke

dbt-migrate:
	@$(COMPOSE) --profile dbt-processing exec -T postgres \
		psql --username "$(POSTGRES_USER)" --dbname "$(CINEFLUX_POSTGRES_DB)" \
		--set=serving_schema="$(POSTGRES_SERVING_SCHEMA)" \
		--set=ON_ERROR_STOP=1 \
		--file=/platform-migrations/002_serving_tables.sql

dbt-run: dbt-migrate
	@$(COMPOSE) --profile dbt-processing run --rm dbt-jobs run

dbt-test:
	@$(COMPOSE) --profile dbt-processing run --rm dbt-jobs test

dbt-docs:
	@$(COMPOSE) --profile dbt-processing run --rm dbt-jobs docs generate

dbt-docs-serve:
	@$(COMPOSE) --profile dbt-processing run --rm --service-ports dbt-jobs \
		docs serve --host 0.0.0.0 --port 8080

storage-index-reset:
	@$(COMPOSE) --profile dbt-processing exec -T postgres \
		psql --username "$(POSTGRES_USER)" --dbname "$(CINEFLUX_POSTGRES_DB)" \
		--set=serving_schema="$(POSTGRES_SERVING_SCHEMA)" \
		--set=ON_ERROR_STOP=1 \
		--file=/platform-migrations/benchmarks/reset_mart_content_trending_score_index.sql

storage-index-create:
	@$(COMPOSE) --profile dbt-processing exec -T postgres \
		psql --username "$(POSTGRES_USER)" --dbname "$(CINEFLUX_POSTGRES_DB)" \
		--set=serving_schema="$(POSTGRES_SERVING_SCHEMA)" \
		--set=ON_ERROR_STOP=1 \
		--file=/platform-migrations/003_serving_indexes.sql

storage-index-benchmark:
	@$(COMPOSE) --profile dbt-processing exec -T postgres \
		psql --username "$(POSTGRES_USER)" --dbname "$(CINEFLUX_POSTGRES_DB)" \
		--set=ON_ERROR_STOP=1 \
		--file=/platform-migrations/benchmarks/mart_content_trending_score.sql
