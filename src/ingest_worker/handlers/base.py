from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar, Protocol, runtime_checkable

from src.shared.db.uow import UnitOfWork
from src.shared.domain.enums import EventType
from src.shared.domain.events import EventEnvelope


class DependencyNotReadyError(RuntimeError):
    pass


@runtime_checkable
class EventHandler(Protocol):
    event_type: ClassVar[EventType]

    def handle(self, uow: UnitOfWork, envelope: EventEnvelope) -> None: ...


class HandlerRegistry:
    def __init__(self, handlers: Iterable[EventHandler]) -> None:
        self._handlers: dict[EventType, EventHandler] = {
            handler.event_type: handler for handler in handlers
        }

    def get(self, event_type: EventType) -> EventHandler:
        handler = self._handlers.get(event_type)
        if handler is None:
            raise KeyError(f"sem handler registrado para {event_type}")
        return handler

    def supports(self, event_type: EventType) -> bool:
        return event_type in self._handlers
