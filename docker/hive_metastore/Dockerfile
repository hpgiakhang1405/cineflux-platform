ARG HIVE_IMAGE=apache/hive:4.1.0

FROM alpine:3.21.3 AS jdbc-driver

ARG POSTGRES_JDBC_VERSION=42.7.7
ARG POSTGRES_JDBC_SHA256=157963d60ae66d607e09466e8c0cdf8087e9cb20d0159899ffca96bca2528460

RUN wget -q \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        -O /postgresql.jar \
    && echo "${POSTGRES_JDBC_SHA256}  /postgresql.jar" | sha256sum -c -

FROM ${HIVE_IMAGE}

COPY --from=jdbc-driver /postgresql.jar /opt/hive/lib/postgresql.jar
