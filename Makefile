SHELL := /bin/bash

COMPOSE := docker compose
PROFILES := messaging storage processing orchestration analytics governance

.DEFAULT_GOAL := help

.PHONY: help env check-profile check-service config up ps logs stop

help:
	@printf '%s\n' \
		'Usage:' \
		'  make env' \
		'  make config PROFILE=<profile>' \
		'  make up PROFILE=<profile>' \
		'  make ps PROFILE=<profile>' \
		'  make logs SERVICE=<service>' \
		'  make stop PROFILE=<profile>' \
		'' \
		'Profiles: $(PROFILES)'

env:
	@bash docker/scripts/create_local_env.sh

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
