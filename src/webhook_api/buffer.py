from __future__ import annotations

import asyncio
import contextlib
import logging

from src.shared.config import Settings
from src.shared.domain.events import EventEnvelope
from src.shared.messaging.ports import EventPublisher

logger = logging.getLogger(__name__)

DRAIN_TIMEOUT_SECONDS = 10


class BufferFullError(RuntimeError):
    pass


class EventBuffer:
    def __init__(self, publisher: EventPublisher, settings: Settings) -> None:
        self._publisher = publisher
        self._settings = settings
        self._queue: asyncio.Queue[EventEnvelope] = asyncio.Queue(
            maxsize=settings.buffer_queue_size
        )
        self._flusher: asyncio.Task[None] | None = None
        self._running = False
        self.published = 0

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    def submit(self, event: EventEnvelope) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull as exc:
            raise BufferFullError("buffer de ingestao cheio") from exc

    async def start(self) -> None:
        self._running = True
        self._flusher = asyncio.create_task(self._flush_periodically(), name="event-buffer")

    async def stop(self) -> None:
        self._running = False
        if self._flusher is not None:
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(self._flusher, timeout=DRAIN_TIMEOUT_SECONDS)
        await self._flush()

    async def _flush_periodically(self) -> None:
        interval_seconds = self._settings.buffer_flush_ms / 1000
        while self._running:
            await asyncio.sleep(interval_seconds)
            try:
                await self._flush()
            except Exception:
                logger.exception("falha ao publicar lote")

    async def _flush(self) -> None:
        batch = self._take_batch()
        if batch:
            self.published += await self._publisher.publish_many(batch)

    def _take_batch(self) -> list[EventEnvelope]:
        batch: list[EventEnvelope] = []
        while len(batch) < self._settings.buffer_max_batch:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch
