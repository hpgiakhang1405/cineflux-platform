# syntax=docker/dockerfile:1.7

ARG PYTHON_RUNTIME_IMAGE
ARG UV_IMAGE

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_RUNTIME_IMAGE} AS builder
COPY --from=uv /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
COPY data_platform/generator/pyproject.toml data_platform/generator/uv.lock data_platform/generator/README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY data_platform/generator/src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM ${PYTHON_RUNTIME_IMAGE}
RUN groupadd --system cineflux \
    && useradd --system --gid cineflux --create-home cineflux
WORKDIR /app
COPY --from=builder --chown=cineflux:cineflux /app/.venv /app/.venv
COPY --chown=cineflux:cineflux data_platform/generator/config ./config
COPY --chown=cineflux:cineflux schemas/avro/playback_events_v1.avsc ./schemas/playback_events_v1.avsc
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1
USER cineflux
ENTRYPOINT ["cineflux-data-generator"]
