"""Generate deterministic playback events and configured stream problems.

Reads: validated shared and stream scenario configuration.
Writes: StreamEnvelope objects containing Avro payloads and transport truth headers.
Runs: the stream publisher runner and quality summary workflow.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, timedelta
from typing import Literal

import numpy as np

from generator.config import SharedConfig, StreamConfig
from generator.ids import content_id, event_id, session_id, user_id
from generator.playback import session_entities, stable_int
from generator.stream.models import StreamEnvelope, StreamEvent
from generator.stream.problems import (
    BurstSchedule,
    DuplicateIndexSelector,
    OutOfOrderIndexSelector,
    RateIndexSelector,
)

STREAM_EVENT_OFFSET = 1_000_000_000_000
STREAM_SESSION_OFFSET = 100_000_000_000


class StreamDataGenerator:
    """Generate a bounded deterministic event stream without external I/O."""

    def __init__(self, shared: SharedConfig, stream: StreamConfig) -> None:
        """Initialize problem index sets and the burst schedule."""
        self._shared = shared
        self._stream = stream
        rng = np.random.default_rng(shared.seed + 500)
        self._invalid_indexes = RateIndexSelector(stream.invalid_payload_rate).select(
            stream.base_event_count, rng
        )
        self._late_indexes = RateIndexSelector(stream.late_arrival_rate).select(
            stream.base_event_count, rng
        )
        self._out_of_order_indexes = OutOfOrderIndexSelector(
            stream.out_of_order_rate, stream.out_of_order_buffer_size
        ).select(stream.base_event_count, rng)
        self._duplicate_indexes = DuplicateIndexSelector(
            stream.duplicate_rate, frozenset(self._invalid_indexes)
        ).select(
            stream.base_event_count,
            rng,
        )
        self._schedule = BurstSchedule(stream.base_rate_per_second, stream.burst)
        self._rng = np.random.default_rng(shared.seed + 501)

    def events(self) -> Iterator[StreamEnvelope]:
        """Yield base events in buffered order followed by selected retry copies."""
        for start in range(0, self._stream.base_event_count, self._stream.out_of_order_buffer_size):
            stop = min(start + self._stream.out_of_order_buffer_size, self._stream.base_event_count)
            buffer = [self._event(index) for index in range(start, stop)]
            self._apply_out_of_order_swaps(buffer, start)
            for envelope in buffer:
                base_index = (
                    int(envelope.event.event_id.removeprefix("evt_")) - 1 - STREAM_EVENT_OFFSET
                )
                yield envelope
                if base_index in self._duplicate_indexes:
                    yield envelope.duplicate()

    def _event(self, index: int) -> StreamEnvelope:
        """Generate one validated base event and its problem flags."""
        scheduled = self._schedule.at(index)
        produced = self._shared.cutover_timestamp.astimezone(UTC) + timedelta(
            hours=2, seconds=scheduled.offset_seconds
        )
        is_late = index in self._late_indexes
        if is_late:
            delay = int(
                self._rng.integers(
                    self._stream.late_arrival_min_seconds,
                    self._stream.late_arrival_max_seconds + 1,
                )
            )
        else:
            delay = int(self._rng.integers(0, 6))
        occurred = produced - timedelta(seconds=delay)
        event_types: tuple[
            Literal["playback_started"],
            Literal["playback_progressed"],
            Literal["playback_completed"],
        ] = (
            "playback_started",
            "playback_progressed",
            "playback_completed",
        )
        session_index_value = STREAM_SESSION_OFFSET + index // 3
        phase = index % 3
        event_type = event_types[phase]
        entities = session_entities(
            self._shared.seed,
            session_index_value,
            self._shared.user_count,
            self._shared.content_count,
        )
        runtime_seconds = 4_800 + stable_int(
            self._shared.seed, session_index_value, 3_201, salt=37
        )
        progress_seconds = 60 + stable_int(
            self._shared.seed, session_index_value, runtime_seconds - 60, salt=43
        )
        if phase == 0:
            position_seconds, watch_seconds = 0, 0
        elif phase == 1:
            position_seconds = progress_seconds
            watch_seconds = progress_seconds
        else:
            position_seconds = runtime_seconds
            watch_seconds = runtime_seconds
        event = StreamEvent(
            event_id=event_id(STREAM_EVENT_OFFSET + index),
            user_id=user_id(entities.user_index),
            content_id=content_id(entities.content_index),
            session_id=session_id(session_index_value),
            event_type=event_type,
            position_seconds=position_seconds,
            watch_seconds=watch_seconds,
            event_timestamp=round(occurred.timestamp() * 1000),
            produced_timestamp=round(produced.timestamp() * 1000),
            payload_schema_version=1,
        )
        return StreamEnvelope(
            event=event,
            simulated_offset_seconds=scheduled.offset_seconds,
            is_burst=scheduled.is_burst,
            is_late=is_late,
            is_out_of_order=index in self._out_of_order_indexes,
            is_invalid=index in self._invalid_indexes,
        )

    def _apply_out_of_order_swaps(self, buffer: list[StreamEnvelope], start_index: int) -> None:
        """Swap selected adjacent events to create observable arrival-order violations."""
        for position in range(0, len(buffer) - 1, 2):
            if start_index + position in self._out_of_order_indexes:
                buffer[position], buffer[position + 1] = buffer[position + 1], buffer[position]
