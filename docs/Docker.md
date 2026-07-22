# Docker and Docker Compose

## Overview

CineFlux uses Docker Compose as the local deployment surface for the data platform. The
stack is divided into profiles so that developers can run one concern at a time instead of
allocating memory to every service simultaneously.

All image versions, exposed ports, database names, bucket names, and local credentials are
defined through environment variables. The public template is `.env.example`; the generated
`.env` file is local-only and must never be committed.

## Prerequisites

- Docker Engine with Docker Compose v2.
- GNU Make.
- Bash, OpenSSL, and standard Unix command-line tools.
- Enough memory for the selected profile. The `governance` profile is the heaviest profile.

Run every command from the repository root.

## Local Environment

Create the local environment once:

```bash
make env
```

The command copies `.env.example` to `.env`, generates local-only credentials, and sets the
file mode to `0600`. It fails without modifying the existing file when `.env` already exists.
Both files must keep the same keys. Local secret values are expected to differ from the
public placeholders; non-secret values should remain aligned unless a developer intentionally
overrides the local deployment.

Validate a profile before starting it:

```bash
make config PROFILE=storage
```

`make config` uses `docker compose config --quiet`, so validation does not print interpolated
secrets.

## Compose Profiles

| Profile | Services | Primary purpose |
|---|---|---|
| `messaging` | Kafka, Schema Registry, Kafka UI | Event transport and schema inspection |
| `storage` | PostgreSQL, MinIO, Hive Metastore, Trino | Object storage, Iceberg catalog, serving storage, and SQL access |
| `processing` | Messaging and storage dependencies, Spark, Spark History Server, Flink | Distributed batch and stream-processing runtime |
| `orchestration` | PostgreSQL, Airflow init, webserver, scheduler | Workflow orchestration runtime |
| `analytics` | Storage dependencies, Superset init, Superset | SQL analytics and visualization runtime |
| `governance` | PostgreSQL, Kafka, Schema Registry, Elasticsearch, DataHub | Metadata storage, search, events, and governance UI |
| `generator-batch` | MinIO, batch Data Generator | Synthetic Parquet source generation |
| `generator-stream` | Kafka, Schema Registry, stream Data Generator | Synthetic event-stream generation |

Start, inspect, read logs, and stop a profile with:

```bash
make up PROFILE=messaging
make ps PROFILE=messaging
make logs SERVICE=kafka
make stop PROFILE=messaging
```

`make stop` stops the selected profile without removing its containers or named volumes.

To inspect one-shot initialization containers as well as running containers, use:

```bash
docker compose --profile messaging ps --all
```

Initialization services must exit with code `0`. Long-running services with healthchecks
must report `healthy` before the profile is considered ready.

## Local Endpoints

| Service | Endpoint | Authentication source |
|---|---|---|
| Kafka | `localhost:9092` | None for the local plaintext listener |
| Schema Registry | `http://localhost:8081` | None |
| Kafka UI | `http://localhost:8080` | None |
| PostgreSQL | `localhost:5432` | `POSTGRES_USER`, `POSTGRES_PASSWORD` |
| MinIO API | `http://localhost:9000` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| MinIO Console | `http://localhost:9001` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| Hive Metastore | `localhost:9083` | Internal Thrift endpoint |
| Trino | `http://localhost:8088` | No local authentication |
| Spark Master UI | `http://localhost:8082` | None |
| Spark History Server | `http://localhost:18080` | None |
| Flink UI | `http://localhost:8083` | None |
| Airflow UI | `http://localhost:8084` | `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD` |
| Elasticsearch | `http://localhost:9200` | Security disabled for local development |
| DataHub GMS | `http://localhost:8086` | Metadata service authentication disabled locally |
| DataHub UI | `http://localhost:9002` | Managed by the DataHub frontend image |
| Superset UI | `http://localhost:8089` | `SUPERSET_ADMIN_USERNAME`, `SUPERSET_ADMIN_PASSWORD` |

Do not capture or publish screenshots that expose values from `.env`.

## Bootstrap Resources

### PostgreSQL

`docker/postgres/init/001_initialize_platform.sh` runs only when PostgreSQL initializes a
new data volume. It creates the application databases:

- `cineflux`
- `hive_metastore`
- `airflow`
- `superset`
- `datahub`

It also creates the `serving` and `streaming` schemas in the `cineflux` database. The
default `postgres` database remains available for administration and maintenance.

Application databases are isolated because Hive Metastore, Airflow, Superset, and DataHub
own their internal schemas. They must not be used for CineFlux business data.

### MinIO

The `minio-init` one-shot service creates these buckets idempotently:

| Bucket | Responsibility |
|---|---|
| `cineflux-landing` | Source files waiting for batch ingestion |
| `cineflux-warehouse` | Iceberg data and metadata files |
| `cineflux-checkpoints` | Durable stream-processing checkpoints and state |

Spark event logs do not use the checkpoint bucket. They are stored in the `spark-events`
named volume so that Spark History Server is available from the first processing run.

### Airflow, Superset, and DataHub

Each application has an idempotent initialization job:

- `airflow-init` migrates the metadata database and creates the local administrator.
- `superset-init` migrates the metadata database, creates the administrator, and initializes
  roles and permissions.
- `datahub-upgrade` creates or upgrades DataHub SQL structures, Kafka topics, schemas, and
  Elasticsearch indices before GMS starts.

## Trino Catalogs

Trino is the unified SQL entry point for the local platform:

- The `iceberg` catalog resolves Iceberg metadata through Hive Metastore and reads objects
  from MinIO using path-style S3 access.
- The `postgresql` catalog exposes the `cineflux` PostgreSQL database, including the
  `serving` and `streaming` schemas.

Validate both catalogs with:

```bash
docker compose exec -T trino trino --execute "SHOW SCHEMAS FROM iceberg"
docker compose exec -T trino trino --execute "SHOW SCHEMAS FROM postgresql"
```

Tools such as DBeaver can connect directly to PostgreSQL for database administration or to
Trino for unified Iceberg and PostgreSQL queries.

## Healthchecks and Startup Ordering

Dependencies use health or successful-completion conditions instead of fixed sleeps. Key
examples include:

- Schema Registry waits for Kafka.
- Kafka UI waits for Kafka and Schema Registry.
- Hive Metastore waits for PostgreSQL and MinIO bucket initialization.
- Trino waits for Hive Metastore and PostgreSQL.
- Spark workers wait for the Spark master.
- Flink TaskManager waits for Flink JobManager.
- Airflow services wait for the Airflow initialization job.
- DataHub GMS waits for a successful DataHub upgrade.
- Superset waits for its initialization job and Trino.

Healthchecks prove process readiness and dependency connectivity. They are not replacements
for later pipeline, data-quality, or performance tests.

## Dockerfile Conventions

Every first-party deployable component must follow these rules:

1. Pin base-image and dependency versions; never use `latest`.
2. Keep one `.dockerignore` at the Docker build-context root so every component build uses
   the same exclusions.
3. Prefer multistage builds for first-party Python components.
4. Install only declared runtime dependencies in production images.
5. Build Python environments inside the image; never copy a local `.venv`.
6. Keep credentials and environment-specific configuration outside the image.
7. Use a non-root runtime user when the upstream image supports it.
8. Verify downloaded build artifacts with a checksum when possible.
9. Use underscore-separated repository directories and hyphen-separated Compose service
   names.

The current compatibility images are intentionally small extensions of pinned upstream
images:

- `cineflux/hive-metastore:4.1.0` adds a checksum-verified PostgreSQL JDBC driver.
- `cineflux/superset:4.1.4` adds pinned PostgreSQL and Trino Python drivers, then returns to
  the upstream `superset` user.

Both custom images use `pull_policy: build`. They are built locally and are not pulled from
a public `cineflux/*` registry namespace.

## Data Generator Image Optimization

The Data Generator is the first first-party image with a baseline and optimized build:

| Image | Dockerfile | Dependency set | Runtime base | Measured size |
|---|---|---|---|---|
| `cineflux/data-generator:baseline` | `data_platform/generator/Dockerfile.baseline` | Runtime dependencies in a single build stage | `python:3.14.5-bookworm` | 1.97 GB |
| `cineflux/data-generator:0.1.0` | `data_platform/generator/Dockerfile` | Runtime dependencies only | `python:3.14.5-slim-bookworm` | 633 MB |

The optimized image uses a multistage build, a slim runtime base, a non-editable wheel, and a
non-root `cineflux` user. Only the locked virtual environment, runtime configuration, and
Avro contract are copied into the final stage.

The two Compose services use `pull_policy: never` because this first-party image is built
locally with `make generator-build` and is not published to a registry.

Reproduce the comparison:

```bash
make generator-build-baseline
make generator-build
docker images --format "{{.Repository}}:{{.Tag}}\t{{.Size}}" \
  | grep 'cineflux/data-generator'
```

```console
cineflux/data-generator:baseline    1.97GB
cineflux/data-generator:0.1.0       633MB
```

The optimized image saves approximately `1.337 GB`, a `67.9%` reduction:

```text
(1.97 GB - 0.633 GB) / 1.97 GB * 100 = 67.9%
```

## Image Listing

Use this command to inspect all local CineFlux images without exposing environment values:

```bash
docker images --format "{{.Repository}}:{{.Tag}}\t{{.Size}}" | grep cineflux
```

## Resource Management

Run profiles independently on memory-constrained developer machines. A practical sequence
is `messaging`, `storage`, `processing`, `orchestration`, `analytics`, then `governance`,
stopping each profile before starting the next.

Inspect container memory without streaming output:

```bash
docker stats --no-stream
```

The governance profile applies configurable hard limits to Elasticsearch and DataHub
containers. Defaults are declared in `.env.example` and can be reduced only after validating
that migrations, GMS, and the frontend still start successfully.

Windows with WSL 2 may retain filesystem cache after containers stop. The Linux `available`
memory value can differ from the value shown by Windows Task Manager. `wsl --shutdown`
releases the WSL virtual machine but also stops every WSL workload and Docker container.

## Validation Commands

Validate every profile without starting containers:

```bash
make config PROFILE=messaging
make config PROFILE=storage
make config PROFILE=processing
make config PROFILE=orchestration
make config PROFILE=analytics
make config PROFILE=governance
make config PROFILE=generator-batch
make config PROFILE=generator-stream
```

Representative runtime checks are:

```bash
curl -fsS http://localhost:8080/actuator/health
curl -fsS http://localhost:9000/minio/health/live
curl -fsS http://localhost:8084/health
curl -fsS http://localhost:8086/health
curl -fsS http://localhost:8089/health
```

For processing runtime validation, submit the Spark example and verify that Spark History
Server lists the completed application:

```bash
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.12-3.5.9.jar \
  10
```

Flink must report one TaskManager and two task slots through `http://localhost:8083/overview`.

## Reset and Cleanup

Remove all CineFlux containers and the Compose network while preserving data:

```bash
docker compose --profile "*" down --remove-orphans
```

Named volumes retain PostgreSQL databases, MinIO objects, Kafka logs, Spark event logs,
Airflow logs, and Elasticsearch data.

To perform an intentionally destructive clean bootstrap, remove the named volumes as well:

```bash
docker compose --profile "*" down --volumes --remove-orphans
```

The second command permanently deletes local platform data. It does not remove `.env`,
source files, or Docker images.

## Troubleshooting

### A service remains unhealthy

Inspect all containers, including initialization jobs, and read the affected service logs:

```bash
docker compose --profile storage ps --all
make logs SERVICE=hive-metastore
```

Do not start another profile until the current profile has no failed or restarting service.

### A custom image tries to pull from Docker Hub

The Hive Metastore and Superset images are local builds. Confirm that the service has
`pull_policy: build`, then rebuild the affected profile:

```bash
docker compose --profile storage build hive-metastore
docker compose --profile analytics build superset-init superset
```

### PostgreSQL resources are missing after changing init scripts

PostgreSQL initialization scripts run only for a new volume. Either apply an explicit
migration or perform the destructive clean bootstrap described above. Never delete a volume
without confirming that its local data can be recreated.

### DataHub does not start

Check `datahub-upgrade` first. GMS intentionally waits for a successful upgrade:

```bash
docker compose --profile governance ps --all
make logs SERVICE=datahub-upgrade
```

Confirm that PostgreSQL, Kafka, Schema Registry, and Elasticsearch are healthy before
diagnosing GMS or the frontend.
