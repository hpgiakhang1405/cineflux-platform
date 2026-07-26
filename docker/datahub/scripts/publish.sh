#!/usr/bin/env bash
# Publish source metadata, validation results, contracts, and governance metadata.

set -euo pipefail

governance_root=/opt/cineflux-governance
rendered_recipes=/tmp/cineflux-recipes

python "${governance_root}/scripts/render_recipes.py" \
    --source "${governance_root}/recipes" \
    --output "${rendered_recipes}"

for recipe in \
    postgresql.yml \
    trino_iceberg.yml \
    trino_postgresql.yml \
    kafka.yml \
    dbt.yml; do
    echo "Publishing ${recipe%.yml} metadata..."
    datahub ingest run --config "${rendered_recipes}/${recipe}"
done

python "${governance_root}/scripts/run_gx.py" \
    --output /tmp/cineflux_gx_results.json
python "${governance_root}/scripts/publish_governance.py" \
    --gx-results /tmp/cineflux_gx_results.json

echo "Governance metadata publication completed."
