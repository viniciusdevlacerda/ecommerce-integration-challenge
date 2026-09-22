from __future__ import annotations

import time
from dataclasses import dataclass

from src.shared.config import Settings
from src.shared.messaging.ports import EventPublisher

MIN_RETRY_AFTER_SECONDS = 1
MAX_RETRY_AFTER_SECONDS = 30


@dataclass(slots=True)
class BackpressureState:
    overloaded: bool
    lag: int
    retry_after_seconds: int


class BackpressureGuard:
    def __init__(self, publisher: EventPublisher, settings: Settings) -> None:
        self._publisher = publisher
        self._settings = settings
        self._last_probe_at = 0.0
        self._state = BackpressureState(
            overloaded=False, lag=0, retry_after_seconds=MIN_RETRY_AFTER_SECONDS
        )

    async def evaluate(self) -> BackpressureState:
        if self._probe_is_due():
            self._state = await self._probe()
        return self._state

    def _probe_is_due(self) -> bool:
        # A medicao e amostrada, nao feita a cada requisicao: consultar o lag em
        # todo webhook transformaria a propria protecao em gargalo.
        elapsed_ms = (time.monotonic() - self._last_probe_at) * 1000
        return elapsed_ms >= self._settings.backpressure_probe_ms

    async def _probe(self) -> BackpressureState:
        self._last_probe_at = time.monotonic()
        lag = await self._publisher.pending_lag()
        limit = self._settings.backpressure_max_lag
        return BackpressureState(
            overloaded=lag >= limit,
            lag=lag,
            retry_after_seconds=self._retry_after_for(lag, limit),
        )

    @staticmethod
    def _retry_after_for(lag: int, limit: int) -> int:
        proportional = lag // max(limit, 1)
        return min(MAX_RETRY_AFTER_SECONDS, max(MIN_RETRY_AFTER_SECONDS, proportional))
