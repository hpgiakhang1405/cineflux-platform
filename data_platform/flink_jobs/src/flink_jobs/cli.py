"""Parse one versioned job configuration and submit the streaming pipeline.

Reads: a YAML configuration path supplied by the Flink CLI.
Writes: one continuously running Flink job graph.
Runs: inside the Flink JobManager through ``flink run -py``.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from flink_jobs.config import load_job_config
from flink_jobs.pipeline import submit_pipeline
from flink_jobs.settings import RuntimeSettings


def build_parser() -> argparse.ArgumentParser:
    """Create the component command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    return parser


def main() -> None:
    """Load validated configuration and submit the Flink pipeline."""
    arguments = build_parser().parse_args()
    submit_pipeline(load_job_config(arguments.config), RuntimeSettings.from_environment())


if __name__ == "__main__":
    main()
