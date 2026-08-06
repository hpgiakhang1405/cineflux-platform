"""CineFlux synthetic batch and streaming data generator.

Reads: YAML scenario configuration and local environment variables.
Writes: Parquet source deliveries to MinIO or Avro playback events to Kafka.
Runs: the ``cineflux-data-generator`` console entrypoint.
"""

from generator.cli import main

__all__ = ["main"]
