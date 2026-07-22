# syntax=docker/dockerfile:1.8
# check=skip=InvalidDefaultArgInFrom

ARG SUPERSET_IMAGE

FROM ${SUPERSET_IMAGE}

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    trino==0.336.0

USER superset
