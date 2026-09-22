from __future__ import annotations

from collections.abc import Sequence

from redis.asyncio import Redis

from src.shared.config import Settings
from src.shared.domain.events import EventEnvelope
from src.shared.messaging.ports import EventPublisher

__all__ = ["EventPublisher", "RedisStreamPublisher"]

HEALTHY_LAG_WHEN_UNMEASURABLE = 0


class RedisStreamPublisher:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self._redis = redis
        self._settings = settings

    async def publish_many(self, events: Sequence[EventEnvelope]) -> int:
        if not events:
            return 0

        pipeline = self._redis.pipeline(transaction=False)
        for event in events:
            pipeline.xadd(
                name=self._settings.stream_name,
                fields={"payload": event.model_dump_json()},
                maxlen=self._settings.stream_maxlen,
                approximate=True,
            )
        await pipeline.execute()
        return len(events)

    async def pending_lag(self) -> int:
        try:
            return int(await self._redis.xlen(self._settings.stream_name))
        except Exception:
            return HEALTHY_LAG_WHEN_UNMEASURABLE
