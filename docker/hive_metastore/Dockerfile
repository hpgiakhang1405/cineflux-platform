# syntax=docker/dockerfile:1.8
# check=skip=InvalidDefaultArgInFrom

ARG HIVE_IMAGE
ARG ALPINE_IMAGE

FROM ${ALPINE_IMAGE} AS jdbc-driver

ARG POSTGRES_JDBC_VERSION=42.7.7
ARG POSTGRES_JDBC_SHA256=157963d60ae66d607e09466e8c0cdf8087e9cb20d0159899ffca96bca2528460

RUN wget -q \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        -O /postgresql.jar \
    && echo "${POSTGRES_JDBC_SHA256}  /postgresql.jar" | sha256sum -c -

FROM ${HIVE_IMAGE}

USER root
COPY --from=jdbc-driver /postgresql.jar /opt/hive/lib/postgresql.jar
COPY docker/hive_metastore/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY docker/hive_metastore/entrypoint.sh /entrypoint.sh
RUN rm /opt/hive/lib/postgresql-9.4.1208.jre7.jar \
    && chmod 755 /entrypoint.sh \
    && cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar /opt/hive/lib/ \
    && cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar /opt/hive/lib/ \
    && chown hive:hive /opt/hive/lib/postgresql.jar \
        /opt/hive/lib/hadoop-aws-*.jar \
        /opt/hive/lib/aws-java-sdk-bundle-*.jar \
        /opt/hadoop/etc/hadoop/core-site.xml
USER hive
