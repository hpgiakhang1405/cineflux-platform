# syntax=docker/dockerfile:1.7
# check=skip=InvalidDefaultArgInFrom

ARG DATAHUB_INGESTION_SLIM_BASE_IMAGE

FROM ${DATAHUB_INGESTION_SLIM_BASE_IMAGE} AS wheels
USER root
COPY docker/datahub/requirements-connectors.txt /tmp/requirements-connectors.txt
COPY docker/datahub/requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir \
    --wheel-dir /tmp/wheels \
    --requirement /tmp/requirements-connectors.txt \
    && pip wheel --no-cache-dir --no-deps \
        --wheel-dir /tmp/wheels \
        --requirement /tmp/requirements.txt

FROM ${DATAHUB_INGESTION_SLIM_BASE_IMAGE}
USER root
RUN --mount=from=wheels,source=/tmp,target=/mnt/build \
    pip install --no-cache-dir --no-index \
        --find-links /mnt/build/wheels \
        --requirement /mnt/build/requirements-connectors.txt \
    && pip uninstall --yes acryl-great-expectations \
    && pip install --no-cache-dir --no-index --no-deps \
        --find-links /mnt/build/wheels \
        --requirement /mnt/build/requirements.txt
COPY --chown=datahub:datahub docker/datahub/recipes /opt/cineflux-governance/recipes
COPY --chown=datahub:datahub docker/datahub/scripts /opt/cineflux-governance/scripts
RUN chmod +x /opt/cineflux-governance/scripts/*.sh
USER datahub
WORKDIR /opt/cineflux-governance
ENTRYPOINT ["/opt/cineflux-governance/scripts/publish.sh"]
