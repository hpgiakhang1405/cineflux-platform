# syntax=docker/dockerfile:1.8

ARG ALPINE_IMAGE
ARG SPARK_IMAGE
ARG UV_IMAGE

FROM ${ALPINE_IMAGE} AS lakehouse-jars

ARG ICEBERG_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_BUNDLE_VERSION
ARG ICEBERG_SHA512=dc3b678071ac489480ba6f4347eee295c80d195dbe6cf27d1f7c172531915504afbba8c6f9253d704f2ad743e2e56be2b4ceb4c6f883bcf07e2a5f66ffb1ce7b
ARG HADOOP_AWS_SHA256=53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a
ARG AWS_SDK_BUNDLE_SHA256=873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5

RUN wget -q \
        "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        -O /iceberg-spark-runtime.jar \
    && echo "${ICEBERG_SHA512}  /iceberg-spark-runtime.jar" | sha512sum -c - \
    && wget -q \
        "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        -O /hadoop-aws.jar \
    && echo "${HADOOP_AWS_SHA256}  /hadoop-aws.jar" | sha256sum -c - \
    && wget -q \
        "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        -O /aws-java-sdk-bundle.jar \
    && echo "${AWS_SDK_BUNDLE_SHA256}  /aws-java-sdk-bundle.jar" | sha256sum -c -

FROM ${UV_IMAGE} AS uv

FROM ${SPARK_IMAGE}

USER root
COPY --from=uv /uv /uvx /bin/
COPY --from=lakehouse-jars /iceberg-spark-runtime.jar /opt/spark/jars/
COPY --from=lakehouse-jars /hadoop-aws.jar /opt/spark/jars/
COPY --from=lakehouse-jars /aws-java-sdk-bundle.jar /opt/spark/jars/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
COPY data_platform/spark_jobs/.python-version \
    data_platform/spark_jobs/pyproject.toml \
    data_platform/spark_jobs/uv.lock \
    data_platform/spark_jobs/README.md ./
RUN uv sync --frozen --no-dev --no-install-project --no-install-package pyspark
COPY data_platform/spark_jobs/src ./src
RUN uv sync --frozen --no-dev --no-editable --no-install-package pyspark \
    && chown -R spark:spark /app
COPY --chown=spark:spark data_platform/spark_jobs/config ./config

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH="/app/src" \
    PYSPARK_DRIVER_PYTHON="/app/.venv/bin/python" \
    PYSPARK_PYTHON="/app/.venv/bin/python" \
    PYTHONUNBUFFERED=1

USER spark
WORKDIR /app
