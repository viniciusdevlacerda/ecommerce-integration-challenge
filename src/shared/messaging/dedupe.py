from __future__ import annotations

from redis import Redis

from src.shared.config import Settings

DEDUPE_KEY_PREFIX = "evt"
CLAIM_WHEN_REDIS_UNAVAILABLE = True


class RedisDeduplicator:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self._redis = redis
        self._ttl_seconds = settings.dedupe_ttl_seconds

    def claim(self, event_id: str) -> bool:
        try:
            reserved = self._redis.set(
                self._key(event_id), "1", nx=True, ex=self._ttl_seconds
            )
        except Exception:
            return CLAIM_WHEN_REDIS_UNAVAILABLE
        return bool(reserved)

    def release(self, event_id: str) -> None:
        """Devolve a reserva quando o processamento falha.

        Sem isso, um evento que falhou ficaria marcado como visto e a reentrega
        seria descartada: perda silenciosa de dado.
        """
        try:
            self._redis.delete(self._key(event_id))
        except Exception:
            pass

    @staticmethod
    def _key(event_id: str) -> str:
        return f"{DEDUPE_KEY_PREFIX}:{event_id}"
