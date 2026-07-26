"""Render environment variables in DataHub recipe keys and values."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import yaml


def expand(value: Any) -> Any:
    """Recursively expand environment variables in YAML keys and string values."""
    if isinstance(value, dict):
        return {expand(str(key)): expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand(item) for item in value]
    if isinstance(value, str):
        expanded = os.path.expandvars(value)
        if "${" in expanded:
            raise RuntimeError(f"Unresolved environment variable in recipe: {expanded}")
        return expanded
    return value


def main() -> None:
    """Render every recipe from the source directory into an ephemeral directory."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for source_path in sorted(args.source.glob("*.yml")):
        document = yaml.safe_load(source_path.read_text(encoding="utf-8"))
        rendered = expand(document)
        output_path = args.output / source_path.name
        output_path.write_text(
            yaml.safe_dump(rendered, sort_keys=False),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
