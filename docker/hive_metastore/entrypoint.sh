#!/usr/bin/env bash
set -euo pipefail

DB_DRIVER="${DB_DRIVER:-derby}"
HIVE_CONF_DIR="${HIVE_HOME}/conf"
export DB_DRIVER HIVE_CONF_DIR

if [[ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]]; then
  find "${HIVE_CUSTOM_CONF_DIR}" -type f -exec ln -sfn {} "${HIVE_CONF_DIR}/" \;
  export HADOOP_CONF_DIR="${HIVE_CONF_DIR}"
  export TEZ_CONF_DIR="${HIVE_CONF_DIR}"
fi

export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1G ${SERVICE_OPTS:-}"

if ! "${HIVE_HOME}/bin/schematool" -dbType "${DB_DRIVER}" -info >/dev/null 2>&1; then
  "${HIVE_HOME}/bin/schematool" -dbType "${DB_DRIVER}" -initSchema
fi

if [[ "${SERVICE_NAME}" == "hiveserver2" ]]; then
  export HADOOP_CLASSPATH="${TEZ_HOME}/*:${TEZ_HOME}/lib/*:${HADOOP_CLASSPATH:-}"
elif [[ "${SERVICE_NAME}" == "metastore" ]]; then
  export METASTORE_PORT="${METASTORE_PORT:-9083}"
fi

exec "${HIVE_HOME}/bin/hive" --skiphadoopversion --skiphbasecp --service "${SERVICE_NAME}"
