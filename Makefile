SHELL := /bin/bash

-include .env

COMPOSE := docker compose
PROFILES := messaging storage processing orchestration analytics governance generator-batch generator-stream
GENERATOR_CONFIG ?= smoke

.DEFAULT_GOAL := help

.PHONY: help env check-local-env check-profile check-service check-delivery check-execution-id config up ps logs stop \
	generator-build generator-build-baseline generator-bootstrap generator-batch \
	generator-stream

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
		'  make generator-stream GENERATOR_CONFIG=smoke|demo EXECUTION_ID=<id>' \
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
