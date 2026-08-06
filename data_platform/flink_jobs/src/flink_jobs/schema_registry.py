"""Register versioned Avro contracts with Confluent Schema Registry.

Reads: local Avro schema files and the Schema Registry HTTP API.
Writes: registered subject versions and compatibility settings.
Runs: on the Flink client before the streaming graph is submitted.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen


class SchemaRegistryGateway:
    """Provide the small Schema Registry surface required by this component."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    def register(self, subject: str, schema_path: Path) -> tuple[dict[str, Any], int]:
        """Register one Avro schema and return its parsed form and global ID."""
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        response = self._request(
            f"/subjects/{quote(subject, safe='')}/versions",
            method="POST",
            body={"schema": json.dumps(schema, separators=(",", ":"))},
        )
        self._request(
            f"/config/{quote(subject, safe='')}",
            method="PUT",
            body={"compatibility": "BACKWARD"},
        )
        schema_id = response.get("id")
        if not isinstance(schema_id, int):
            raise RuntimeError(f"Schema Registry did not return an ID for {subject}")
        return schema, schema_id

    def _request(
        self, path: str, *, method: str, body: dict[str, object]
    ) -> dict[str, Any]:
        """Send one JSON request and require a JSON object response."""
        request = Request(
            f"{self._base_url}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            method=method,
        )
        with urlopen(request, timeout=15) as response:
            values = json.load(response)
        if not isinstance(values, dict):
            raise RuntimeError(f"Unexpected Schema Registry response for {path}")
        return values
