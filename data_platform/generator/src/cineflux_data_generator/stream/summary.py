"""Summarize one generated and broker-acknowledged stream execution.

Reads: incremental envelope counters and Kafka delivery acknowledgments.
Writes: a JSON-compatible metrics dictionary for terminal evidence.
Runs: automatically after the ``stream`` command publishes all messages.
"""

from __future__ import annotations

from cineflux_data_generator.config import SharedConfig, StreamConfig
from cineflux_data_generator.stream.runner import StreamRunResult


def build_stream_summary(
    shared: SharedConfig,
    stream: StreamConfig,
    execution_id: str,
    generated: StreamRunResult,
) -> dict[str, object]:
    """Compare generated stream problem rates with configuration."""
    total = generated.generated_messages
    valid_rows = total - generated.invalid_messages
    return {
        "summary_version": 1,
        "mode": "stream",
        "scenario": stream.scenario_name,
        "execution_id": execution_id,
        "topic": stream.topic,
        "delivery": {
            "generated_messages": total,
            "acknowledged_messages": generated.acknowledged_messages,
            "failed_messages": generated.failed_messages,
        },
        "quality": {
            "cutover": {
                "rule": f"event_timestamp >= {shared.cutover_timestamp.isoformat()}",
                "violations": generated.cutover_violations,
            },
            "burst": {
                "cycle_seconds": stream.burst.cycle_seconds,
                "duration_ratio": stream.burst.duration_ratio,
                "multiplier": stream.burst.multiplier,
                "observed_count": generated.burst_messages,
                "observed_rate": _rate(generated.burst_messages, total),
            },
            "late_arrival": _configured_rate(
                stream.late_arrival_rate, generated.late_messages, total
            ),
            "out_of_order": _configured_rate(
                stream.out_of_order_rate, generated.out_of_order_messages, total
            ),
            "duplicates": {
                **_configured_rate(stream.duplicate_rate, generated.duplicate_messages, total),
                "rows_before_dedup": valid_rows,
                "duplicate_rows_before_dedup": generated.duplicate_messages,
                "rows_after_dedup": valid_rows - generated.duplicate_messages,
                "duplicate_rows_after_dedup": 0,
            },
            "invalid_payload": _configured_rate(
                stream.invalid_payload_rate, generated.invalid_messages, total
            ),
        },
    }


def _configured_rate(configured: float, observed: int, total: int) -> dict[str, object]:
    """Build one configured-versus-observed stream metric."""
    return {
        "configured_rate": configured,
        "observed_count": observed,
        "observed_rate": _rate(observed, total),
    }


def _rate(numerator: int, denominator: int) -> float:
    """Return a stable six-decimal rate with zero-safe division."""
    return round(numerator / denominator, 6) if denominator else 0.0
