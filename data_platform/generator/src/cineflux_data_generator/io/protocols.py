"""Define generator I/O protocols used by pure business logic.

Reads: serialized data and destination metadata from runners.
Writes: implementation-specific external storage or messaging systems.
Runs: dependency composition in generator runners.
"""

from dataclasses import dataclass
from typing import Protocol

from cineflux_data_generator.stream.models import StreamEnvelope, StreamEvent


@dataclass(frozen=True)
class DeliveryResult:
    """Acknowledgment counts returned by an event publisher."""

    acknowledged_messages: int
    failed_messages: int


class ObjectWriter(Protocol):
    """Write immutable objects to source landing storage."""

    @property
    def bucket(self) -> str:
        """Return the configured landing bucket name."""
        ...

    def write(self, object_name: str, data: bytes, content_type: str) -> bool:
        """Write an object and return whether new bytes were uploaded."""
        ...


class EventPublisher(Protocol):
    """Publish one prepared stream execution to an external event transport."""

    def prepare(self, probe: StreamEvent) -> None:
        """Prepare the destination and payload contract using a valid probe event."""
        ...

    def publish(self, envelope: StreamEnvelope, execution_id: str) -> None:
        """Publish one event envelope for an isolated execution."""
        ...

    def flush(self) -> DeliveryResult:
        """Wait for all buffered messages and return delivery acknowledgments."""
        ...
