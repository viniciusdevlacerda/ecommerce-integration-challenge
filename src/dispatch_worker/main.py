from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import socket
import uuid
from datetime import timedelta

import httpx

from src.dispatch_worker.erp_client import (
    ErpClient,
    ErpPermanentError,
    ErpTemporaryError,
    backoff_delay,
)
from src.dispatch_worker.outbox_reader import OutboxItem, OutboxReader, utcnow
from src.shared.config import Settings, get_settings
from src.shared.db.engine import get_session_factory
from src.shared.db.uow import UnitOfWork
from src.shared.logging import configure_logging

logger = logging.getLogger(__name__)

RELEASE_STALE_EVERY_N_CYCLES = 60
MAX_HTTP_CONNECTIONS = 20
MAX_KEEPALIVE_CONNECTIONS = 10


class DispatchWorker:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session_factory = get_session_factory(settings)
        self._worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._reader = OutboxReader(self._worker_id)
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        logger.info("dispatch-worker iniciado", extra={"worker_id": self._worker_id})

        async with self._http_client() as http_client:
            erp = ErpClient(http_client, self._settings)
            await self._poll_until_stopped(erp)

        logger.info("dispatch-worker encerrado")

    def _http_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._settings.erp_base_url,
            limits=httpx.Limits(
                max_connections=MAX_HTTP_CONNECTIONS,
                max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
            ),
        )

    async def _poll_until_stopped(self, erp: ErpClient) -> None:
        idle_interval = self._settings.dispatch_poll_interval_ms / 1000
        cycle = 0

        while not self._stop.is_set():
            cycle += 1
            if cycle % RELEASE_STALE_EVERY_N_CYCLES == 0:
                await asyncio.to_thread(self._release_abandoned)

            items = await asyncio.to_thread(self._claim_batch)
            if not items:
                await asyncio.sleep(idle_interval)
                continue

            await asyncio.gather(*(self._dispatch(erp, item) for item in items))

    def _claim_batch(self) -> list[OutboxItem]:
        session = self._session_factory()
        try:
            return self._reader.claim(session, self._settings.dispatch_batch_size)
        finally:
            session.close()

    def _release_abandoned(self) -> None:
        session = self._session_factory()
        try:
            released = OutboxReader.release_abandoned(session)
            if released:
                logger.warning("mensagens presas liberadas", extra={"count": released})
        finally:
            session.close()

    async def _dispatch(self, erp: ErpClient, item: OutboxItem) -> None:
        try:
            response = await erp.send_order(
                idempotency_key=f"outbox-{item.outbox_id}", payload=item.payload
            )
        except ErpTemporaryError as exc:
            await asyncio.to_thread(self._reschedule, item, str(exc))
        except ErpPermanentError as exc:
            await asyncio.to_thread(self._to_dead_letter, item, str(exc))
        else:
            await asyncio.to_thread(self._mark_sent, item)
            logger.info(
                "pedido despachado ao ERP",
                extra={
                    "order_uid": item.aggregate_id,
                    "erp_order_id": response.erp_order_id,
                    "duplicate": response.duplicate,
                    "attempts": item.attempts,
                },
            )

    def _reschedule(self, item: OutboxItem, error: str) -> None:
        if item.attempts >= self._settings.dispatch_max_attempts:
            self._to_dead_letter(item, f"tentativas esgotadas: {error}")
            return

        delay_seconds = backoff_delay(item.attempts, self._settings)
        with UnitOfWork(self._session_factory) as uow:
            uow.outbox.mark_retry(
                item.outbox_id,
                next_attempt_at=utcnow() + timedelta(seconds=delay_seconds),
                error=error,
            )
            uow.commit()

        logger.warning(
            "falha temporaria no ERP, reagendado",
            extra={
                "order_uid": item.aggregate_id,
                "attempt": item.attempts,
                "retry_in_seconds": round(delay_seconds, 2),
                "error": error,
            },
        )

    def _mark_sent(self, item: OutboxItem) -> None:
        with UnitOfWork(self._session_factory) as uow:
            uow.outbox.mark_sent(item.outbox_id)
            uow.commit()

    def _to_dead_letter(self, item: OutboxItem, error: str) -> None:
        with UnitOfWork(self._session_factory) as uow:
            uow.outbox.mark_dead_letter(item.outbox_id, error=error)
            uow.commit()
        logger.error(
            "mensagem enviada para DLQ",
            extra={"order_uid": item.aggregate_id, "error": error},
        )


async def _main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    worker = DispatchWorker(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, worker.request_stop)

    await worker.run()


if __name__ == "__main__":
    asyncio.run(_main())
