"""Publish generated stream events with optional wall-clock pacing.

Reads: StreamDataGenerator envelopes and a KafkaEventPublisher gateway.
Writes: one isolated Kafka execution plus structured throughput logs.
Runs: the ``stream`` command mode.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from itertools import chain

from generator.io.protocols import EventPublisher
from generator.stream.generator import StreamDataGenerator

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class StreamRunResult:
    """Generation-side delivery and cutover counters."""

    generated_messages: int
    acknowledged_messages: int
    failed_messages: int
    cutover_violations: int
    burst_messages: int
    late_messages: int
    out_of_order_messages: int
    duplicate_messages: int
    invalid_messages: int


class StreamRunner:
    """Coordinate stream generation, pacing, and Kafka delivery."""

    def __init__(
        self,
        generator: StreamDataGenerator,
        publisher: EventPublisher,
        pace_events: bool,
        cutover_timestamp: datetime,
    ) -> None:
        """Create a stream runner with explicit generator and publisher dependencies."""
        self._generator = generator
        self._publisher = publisher
        self._pace_events = pace_events
        self._cutover_timestamp_ms = round(cutover_timestamp.timestamp() * 1000)

    def run(self, execution_id: str) -> StreamRunResult:
        """Publish a complete deterministic stream execution."""
        events = self._generator.events()
        try:
            first = next(events)
        except StopIteration as exc:
            raise RuntimeError("Stream configuration produced no events") from exc
        self._publisher.prepare(first.event)
        started = time.monotonic()
        count = 0
        cutover_violations = 0
        burst_messages = 0
        late_messages = 0
        out_of_order_messages = 0
        duplicate_messages = 0
        invalid_messages = 0
        for envelope in chain((first,), events):
            if self._pace_events:
                target = started + envelope.simulated_offset_seconds
                remaining = target - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
            self._publisher.publish(envelope, execution_id)
            count += 1
            cutover_violations += int(
                envelope.event.event_timestamp < self._cutover_timestamp_ms
            )
            burst_messages += int(envelope.is_burst)
            late_messages += int(envelope.is_late)
            out_of_order_messages += int(envelope.is_out_of_order)
            duplicate_messages += int(envelope.is_duplicate)
            invalid_messages += int(envelope.is_invalid)
        delivery = self._publisher.flush()
        if delivery.acknowledged_messages != count:
            raise RuntimeError(
                f"Expected {count} Kafka acknowledgments, "
                f"received {delivery.acknowledged_messages}"
            )
        elapsed = max(time.monotonic() - started, 1e-9)
        LOGGER.info(
            "stream_generation_completed execution_id=%s messages=%d elapsed_seconds=%.3f "
            "messages_per_second=%.2f",
            execution_id,
            count,
            elapsed,
            count / elapsed,
        )
        return StreamRunResult(
            generated_messages=count,
            acknowledged_messages=delivery.acknowledged_messages,
            failed_messages=delivery.failed_messages,
            cutover_violations=cutover_violations,
            burst_messages=burst_messages,
            late_messages=late_messages,
            out_of_order_messages=out_of_order_messages,
            duplicate_messages=duplicate_messages,
            invalid_messages=invalid_messages,
        )
