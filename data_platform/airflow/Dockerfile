# syntax=docker/dockerfile:1.7

ARG AIRFLOW_BASE_IMAGE
ARG SPARK_JOBS_IMAGE

FROM ${SPARK_JOBS_IMAGE} AS spark-jobs

FROM ${AIRFLOW_BASE_IMAGE}

ARG AIRFLOW_VERSION
ARG AIRFLOW_PYTHON_VERSION
ARG AIRFLOW_CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON_VERSION}.txt

USER root
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless zip \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY --chown=airflow:root data_platform/airflow/requirements.txt /tmp/requirements.txt
RUN curl -fsSL "${AIRFLOW_CONSTRAINTS_URL}" \
        | grep -vE '^(py4j|pyspark)==' > /tmp/airflow-constraints.txt \
    && pip install --no-cache-dir \
        --constraint /tmp/airflow-constraints.txt \
        --requirement /tmp/requirements.txt \
    && rm -f /tmp/airflow-constraints.txt /tmp/requirements.txt

USER root
COPY --chown=airflow:root data_platform/airflow/dags /opt/airflow/dags
COPY --chown=airflow:root data_platform/airflow/scripts /opt/airflow/scripts
COPY --chown=airflow:root data_platform/spark_jobs/src /opt/airflow/jobs/spark_src
COPY --chown=airflow:root data_platform/spark_jobs/config /opt/airflow/jobs/spark_config
COPY --from=spark-jobs /opt/spark/jars/iceberg-spark-runtime.jar /opt/airflow/jars/
COPY --from=spark-jobs /opt/spark/jars/hadoop-aws.jar /opt/airflow/jars/
COPY --from=spark-jobs /opt/spark/jars/aws-java-sdk-bundle.jar /opt/airflow/jars/
RUN cd /opt/airflow/jobs/spark_src \
    && zip -qr /opt/airflow/jobs/spark_jobs.zip spark_jobs \
    && chown -R airflow:root /opt/airflow/jobs /opt/airflow/jars \
    && chmod +x /opt/airflow/scripts/bootstrap_airflow.sh

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1

USER airflow
