from __future__ import annotations

import logging
import socket
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from src.shared.config import Settings

logger = logging.getLogger(__name__)

GROUP_ALREADY_EXISTS = "BUSYGROUP"
NEW_STREAM_POSITION = ">"
DEAD_LETTER_SUFFIX = ":dlq"
DEAD_LETTER_MAXLEN = 100_000
FIRST_DELIVERY = 1


@dataclass(frozen=True, slots=True)
class StreamMessage:
    message_id: str
    payload: str
    delivery_count: int


class RedisStreamConsumer:
    def __init__(
        self, redis: Redis, settings: Settings, consumer_name: str | None = None
    ) -> None:
        self._redis = redis
        self._settings = settings
        self._name = consumer_name or _unique_consumer_name()
        self._dead_letter_stream = f"{settings.stream_name}{DEAD_LETTER_SUFFIX}"

    @property
    def name(self) -> str:
        return self._name

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                name=self._settings.stream_name,
                groupname=self._settings.consumer_group,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if GROUP_ALREADY_EXISTS not in str(exc):
                raise
        else:
            logger.info("consumer group criado", extra={"group": self._settings.consumer_group})

    async def read(self) -> list[StreamMessage]:
        entries = await self._redis.xreadgroup(
            groupname=self._settings.consumer_group,
            consumername=self._name,
            streams={self._settings.stream_name: NEW_STREAM_POSITION},
            count=self._settings.consumer_batch_size,
            block=self._settings.consumer_block_ms,
        )
        return self._to_messages(entries)

    async def reclaim_abandoned(self) -> list[StreamMessage]:
        delivery_counts = await self._pending_delivery_counts()
        if not delivery_counts:
            return []

        claimed = await self._redis.xclaim(
            name=self._settings.stream_name,
            groupname=self._settings.consumer_group,
            consumername=self._name,
            min_idle_time=self._settings.consumer_idle_reclaim_ms,
            message_ids=list(delivery_counts),
        )

        messages = [
            StreamMessage(
                message_id=_as_str(message_id),
                payload=_payload_of(fields),
                delivery_count=delivery_counts.get(_as_str(message_id), FIRST_DELIVERY),
            )
            for message_id, fields in claimed
        ]
        if messages:
            logger.warning("mensagens reivindicadas", extra={"count": len(messages)})
        return messages

    async def ack(self, message_ids: Sequence[str]) -> None:
        if message_ids:
            await self._redis.xack(
                self._settings.stream_name, self._settings.consumer_group, *message_ids
            )

    async def send_to_dead_letter(self, message: StreamMessage, reason: str) -> None:
        await self._redis.xadd(
            self._dead_letter_stream,
            {"payload": message.payload, "reason": reason[:500]},
            maxlen=DEAD_LETTER_MAXLEN,
            approximate=True,
        )
        await self.ack([message.message_id])
        logger.error("mensagem enviada para DLQ", extra={"reason": reason})

    async def _pending_delivery_counts(self) -> dict[str, int]:
        try:
            pending = await self._redis.xpending_range(
                name=self._settings.stream_name,
                groupname=self._settings.consumer_group,
                min="-",
                max="+",
                count=self._settings.consumer_batch_size,
                idle=self._settings.consumer_idle_reclaim_ms,
            )
        except ResponseError:
            return {}

        return {
            _as_str(entry["message_id"]): int(entry["times_delivered"]) for entry in pending
        }

    @staticmethod
    def _to_messages(raw: object) -> list[StreamMessage]:
        if not raw:
            return []
        return [
            StreamMessage(
                message_id=_as_str(message_id),
                payload=_payload_of(fields),
                delivery_count=FIRST_DELIVERY,
            )
            for _stream, entries in raw  # type: ignore[union-attr]
            for message_id, fields in entries
        ]


def _unique_consumer_name() -> str:
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


def _payload_of(fields: dict[object, object]) -> str:
    return _as_str(fields.get(b"payload") or fields.get("payload") or "")


def _as_str(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
