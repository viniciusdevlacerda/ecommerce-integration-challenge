from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Sequence

from pydantic import ValidationError
from redis import Redis as SyncRedis
from redis.asyncio import Redis

from src.ingest_worker.handlers.base import HandlerRegistry
from src.ingest_worker.handlers.client_handler import ClientUpsertedHandler
from src.ingest_worker.handlers.invoice_handler import InvoiceIssuedHandler
from src.ingest_worker.handlers.order_handler import OrderCreatedHandler
from src.ingest_worker.handlers.payment_handler import PaymentUpdatedHandler
from src.ingest_worker.processor import EventProcessor
from src.shared.config import Settings, get_settings
from src.shared.db.engine import get_session_factory
from src.shared.domain.events import EventEnvelope
from src.shared.logging import configure_logging
from src.shared.messaging.consumer import RedisStreamConsumer, StreamMessage
from src.shared.messaging.dedupe import RedisDeduplicator

logger = logging.getLogger(__name__)

RECLAIM_EVERY_N_CYCLES = 20

DeadLetter = tuple[StreamMessage, str]


def build_registry() -> HandlerRegistry:
    return HandlerRegistry(
        [
            ClientUpsertedHandler(),
            OrderCreatedHandler(),
            PaymentUpdatedHandler(),
            InvoiceIssuedHandler(),
        ]
    )


class IngestWorker:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._redis = Redis.from_url(settings.redis_url, decode_responses=False)
        self._consumer = RedisStreamConsumer(self._redis, settings)
        self._processor = EventProcessor(
            session_factory=get_session_factory(settings),
            registry=build_registry(),
            deduplicator=RedisDeduplicator(
                SyncRedis.from_url(settings.redis_url, decode_responses=False), settings
            ),
        )
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        await self._consumer.ensure_group()
        logger.info("ingest-worker iniciado", extra={"consumer": self._consumer.name})

        cycle = 0
        while not self._stop.is_set():
            cycle += 1
            messages = await self._next_messages(cycle)
            if messages:
                await self._handle(messages)

        await self._redis.aclose()
        logger.info("ingest-worker encerrado")

    async def _next_messages(self, cycle: int) -> list[StreamMessage]:
        messages = await self._consumer.read()
        if cycle % RECLAIM_EVERY_N_CYCLES == 0:
            messages += await self._consumer.reclaim_abandoned()
        return messages

    async def _handle(self, messages: Sequence[StreamMessage]) -> None:
        acknowledgeable, dead_letters = await asyncio.to_thread(self._persist, messages)

        for message, reason in dead_letters:
            await self._consumer.send_to_dead_letter(message, reason)

        # ACK somente depois do commit: a entrega vira at-least-once e, somada a
        # idempotencia do processor, o efeito liquido e exactly-once.
        await self._consumer.ack(acknowledgeable)

    def _persist(
        self, messages: Sequence[StreamMessage]
    ) -> tuple[list[str], list[DeadLetter]]:
        acknowledgeable: list[str] = []
        dead_letters: list[DeadLetter] = []
        outcomes: dict[str, int] = {}

        for message in messages:
            envelope = self._parse(message)
            if envelope is None:
                dead_letters.append((message, "payload invalido"))
                continue

            result = self._processor.process(envelope)
            outcomes[result.outcome.value] = outcomes.get(result.outcome.value, 0) + 1

            if result.should_ack:
                acknowledgeable.append(message.message_id)
            elif self._deliveries_exhausted(message):
                dead_letters.append((message, f"tentativas esgotadas: {result.detail}"))

        if outcomes:
            logger.info("lote processado", extra=outcomes)
        return acknowledgeable, dead_letters

    def _deliveries_exhausted(self, message: StreamMessage) -> bool:
        return message.delivery_count >= self._settings.consumer_max_delivery

    @staticmethod
    def _parse(message: StreamMessage) -> EventEnvelope | None:
        try:
            return EventEnvelope.model_validate_json(message.payload)
        except ValidationError as exc:
            logger.error("payload invalido descartado", extra={"error": str(exc)[:300]})
            return None


async def _main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    worker = IngestWorker(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, worker.request_stop)

    await worker.run()


if __name__ == "__main__":
    asyncio.run(_main())
