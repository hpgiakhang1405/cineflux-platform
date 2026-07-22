"""Expose deterministic Kafka playback generation and verification logic.

Reads: validated shared and stream scenario configuration.
Writes: stream envelopes and quality metrics for publisher runners.
Runs: imports from the stream generator package.
"""
