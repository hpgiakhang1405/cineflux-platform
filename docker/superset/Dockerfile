ARG SUPERSET_IMAGE=apache/superset:4.1.4

FROM ${SUPERSET_IMAGE}

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    trino==0.336.0

USER superset
