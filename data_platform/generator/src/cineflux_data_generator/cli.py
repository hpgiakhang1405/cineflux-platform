"""Expose generator modes and automatic JSON summaries through a command-line interface.

Reads: command arguments, YAML configuration, and runtime environment variables.
Writes: MinIO source files, Kafka messages, or JSON quality summaries.
Runs: the ``cineflux-data-generator`` console script and Docker entrypoint.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from cineflux_data_generator.modes import GeneratorModeFactory


def main() -> None:
    """Parse command arguments, compose dependencies, and execute one command."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s level=%(levelname)s logger=%(name)s message=%(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    parser = _build_parser()
    args = parser.parse_args()
    try:
        mode = GeneratorModeFactory.create(
            args.command,
            args.config,
            delivery_id=getattr(args, "delivery_id", None),
            execution_id=getattr(args, "execution_id", None),
        )
        summary = mode.run()
        print(json.dumps(summary, indent=2, sort_keys=True))
    except (OSError, RuntimeError, ValueError) as exc:
        parser.exit(2, f"error: {exc}\n")


def _build_parser() -> argparse.ArgumentParser:
    """Create the complete command-line parser."""
    parser = argparse.ArgumentParser(description="Generate CineFlux synthetic source data.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser("bootstrap", help="Generate historical batch events.")
    bootstrap.add_argument("--config", type=Path, required=True)

    recurring = subparsers.add_parser(
        "recurring_batch", help="Generate a recurring master-data delivery."
    )
    recurring.add_argument("--config", type=Path, required=True)
    recurring.add_argument("--delivery-id", required=True)

    stream = subparsers.add_parser("stream", help="Publish playback events to Kafka.")
    stream.add_argument("--config", type=Path, required=True)
    stream.add_argument("--execution-id", required=True)

    return parser
