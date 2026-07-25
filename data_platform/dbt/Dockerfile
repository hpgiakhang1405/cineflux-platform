# syntax=docker/dockerfile:1.7

ARG DBT_PYTHON_RUNTIME_IMAGE
ARG UV_IMAGE

FROM ${UV_IMAGE} AS uv

FROM ${DBT_PYTHON_RUNTIME_IMAGE} AS builder
COPY --from=uv /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
COPY data_platform/dbt/.python-version \
    data_platform/dbt/pyproject.toml \
    data_platform/dbt/uv.lock \
    data_platform/dbt/README.md ./
RUN uv sync --frozen --no-dev

FROM ${DBT_PYTHON_RUNTIME_IMAGE}
RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system cineflux \
    && useradd --system --uid 1000 --gid cineflux --create-home cineflux
WORKDIR /app
COPY --from=builder --chown=cineflux:cineflux /app/.venv /app/.venv
COPY --chown=cineflux:cineflux data_platform/dbt/dbt_project.yml ./dbt_project.yml
COPY --chown=cineflux:cineflux data_platform/dbt/profiles ./profiles
COPY --chown=cineflux:cineflux data_platform/dbt/macros ./macros
COPY --chown=cineflux:cineflux data_platform/dbt/models ./models
COPY --chown=cineflux:cineflux data_platform/dbt/tests ./tests
RUN mkdir -p /app/target \
    && chown -R cineflux:cineflux /app
ENV PATH="/app/.venv/bin:${PATH}" \
    DBT_PROFILES_DIR=/app/profiles \
    DBT_LOG_PATH=/app/target/logs \
    PYTHONUNBUFFERED=1
USER cineflux
ENTRYPOINT ["dbt"]
CMD ["--help"]
