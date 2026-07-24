# syntax=docker/dockerfile:1.7

ARG FLINK_BASE_IMAGE
ARG UV_IMAGE

FROM ${UV_IMAGE} AS uv

FROM ${FLINK_BASE_IMAGE} AS builder
ARG PYTHON_VERSION
COPY --from=uv /uv /uvx /bin/
USER root
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PROJECT_ENVIRONMENT=/opt/flink/venv
COPY data_platform/flink_jobs/pyproject.toml data_platform/flink_jobs/uv.lock data_platform/flink_jobs/README.md ./
RUN uv python install "${PYTHON_VERSION}" \
    && uv sync --frozen --no-dev --no-install-project
COPY data_platform/flink_jobs/src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM ${FLINK_BASE_IMAGE}
ARG FLINK_VERSION
ARG FLINK_KAFKA_CONNECTOR_VERSION
ARG FLINK_JDBC_CONNECTOR_VERSION
ARG POSTGRES_JDBC_VERSION
ARG ROCKSDB_JAR_SHA1
ARG KAFKA_CONNECTOR_JAR_SHA1
ARG JDBC_CONNECTOR_JAR_SHA1
ARG POSTGRES_JDBC_JAR_SHA1
USER root
RUN set -eux; \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-statebackend-rocksdb/${FLINK_VERSION}/flink-statebackend-rocksdb-${FLINK_VERSION}.jar" -o /tmp/rocksdb.jar; \
    echo "${ROCKSDB_JAR_SHA1}  /tmp/rocksdb.jar" | sha1sum -c -; \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" -o /tmp/kafka.jar; \
    echo "${KAFKA_CONNECTOR_JAR_SHA1}  /tmp/kafka.jar" | sha1sum -c -; \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${FLINK_JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${FLINK_JDBC_CONNECTOR_VERSION}.jar" -o /tmp/jdbc.jar; \
    echo "${JDBC_CONNECTOR_JAR_SHA1}  /tmp/jdbc.jar" | sha1sum -c -; \
    curl -fsSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" -o /tmp/postgres.jar; \
    echo "${POSTGRES_JDBC_JAR_SHA1}  /tmp/postgres.jar" | sha1sum -c -; \
    mv /tmp/rocksdb.jar "/opt/flink/lib/flink-statebackend-rocksdb-${FLINK_VERSION}.jar"; \
    mv /tmp/kafka.jar "/opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"; \
    mv /tmp/jdbc.jar "/opt/flink/lib/flink-connector-jdbc-${FLINK_JDBC_CONNECTOR_VERSION}.jar"; \
    mv /tmp/postgres.jar "/opt/flink/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp "/opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar" /opt/flink/plugins/s3-fs-hadoop/; \
    mkdir -p /opt/flink/state /app
COPY --from=builder /opt/python /opt/python
COPY --from=builder /opt/flink/venv /opt/flink/venv
COPY --chown=flink:flink data_platform/flink_jobs/config /app/config
COPY --chown=flink:flink data_platform/flink_jobs/src /app/src
COPY --chown=flink:flink schemas/avro/playback_events_v1.avsc /app/schemas/playback_events_v1.avsc
COPY --chown=flink:flink schemas/avro/playback_events_dlq_v1.avsc /app/schemas/playback_events_dlq_v1.avsc
RUN chown -R flink:flink /opt/python /opt/flink/venv /opt/flink/state
ENV PATH="/opt/flink/venv/bin:${PATH}" \
    PYFLINK_CLIENT_EXECUTABLE=/opt/flink/venv/bin/python \
    PYTHONUNBUFFERED=1
USER flink
