#!/usr/bin/env bash
# Generate a local Docker Compose environment file from the public template.

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
template_path="${repository_root}/.env.example"
environment_path="${repository_root}/.env"

if [[ -e "${environment_path}" ]]; then
    echo ".env already exists; remove it explicitly before generating a replacement." >&2
    exit 1
fi

cp "${template_path}" "${environment_path}"

replace_value() {
    local key="$1"
    local value="$2"
    sed -i "s|^${key}=.*$|${key}=${value}|" "${environment_path}"
}

random_hex() {
    openssl rand -hex "$1"
}

fernet_key="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n')"

replace_value POSTGRES_PASSWORD "$(random_hex 24)"
replace_value MINIO_ROOT_PASSWORD "$(random_hex 24)"
replace_value AIRFLOW_ADMIN_PASSWORD "$(random_hex 18)"
replace_value AIRFLOW_FERNET_KEY "${fernet_key}"
replace_value AIRFLOW_SECRET_KEY "$(random_hex 32)"
replace_value SUPERSET_ADMIN_PASSWORD "$(random_hex 18)"
replace_value SUPERSET_SECRET_KEY "$(random_hex 32)"
replace_value DATAHUB_TOKEN_SERVICE_SIGNING_KEY "$(random_hex 32)"
replace_value DATAHUB_TOKEN_SERVICE_SALT "$(random_hex 16)"
replace_value DATAHUB_SYSTEM_CLIENT_SECRET "$(random_hex 32)"
replace_value DATAHUB_FRONTEND_SECRET "$(random_hex 32)"

chmod 600 "${environment_path}"
echo "Created ${environment_path} with local-only credentials."
