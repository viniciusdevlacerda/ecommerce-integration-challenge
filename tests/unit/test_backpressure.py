"""Backpressure: acima do limite, a borda tem que pedir para o parceiro esperar."""

from __future__ import annotations

from src.shared.config import Settings
from src.webhook_api.backpressure import BackpressureGuard


class FakePublisher:
    def __init__(self, lag: int) -> None:
        self.lag = lag
        self.probes = 0

    async def publish_many(self, events) -> int:  # type: ignore[no-untyped-def]
        return len(events)

    async def pending_lag(self) -> int:
        self.probes += 1
        return self.lag


def _settings(**overrides: object) -> Settings:
    return Settings(mssql_password="x", **overrides)  # type: ignore[arg-type]


async def test_fila_saudavel_nao_aciona_backpressure() -> None:
    guard = BackpressureGuard(FakePublisher(lag=10), _settings(backpressure_max_lag=1000))
    state = await guard.evaluate()

    assert state.overloaded is False


async def test_fila_atrasada_aciona_backpressure() -> None:
    guard = BackpressureGuard(FakePublisher(lag=5000), _settings(backpressure_max_lag=1000))
    state = await guard.evaluate()

    assert state.overloaded is True
    assert state.retry_after_seconds >= 1


async def test_medicao_e_amostrada_e_nao_por_requisicao() -> None:
    """Medir a cada webhook transformaria a protecao em gargalo."""
    publisher = FakePublisher(lag=10)
    guard = BackpressureGuard(publisher, _settings(backpressure_probe_ms=10_000))

    for _ in range(50):
        await guard.evaluate()

    assert publisher.probes == 1
