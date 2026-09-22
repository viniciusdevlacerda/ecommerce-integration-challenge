from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from src.shared.domain.events import EventEnvelope


@runtime_checkable
class EventPublisher(Protocol):
    async def publish_many(self, events: Sequence[EventEnvelope]) -> int: ...

    async def pending_lag(self) -> int: ...
