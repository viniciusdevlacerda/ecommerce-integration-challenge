"""O buffer precisa garantir duas coisas: agrupa, e recusa quando enche."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.shared.config import Settings
from src.shared.domain.events import EventEnvelope
from src.webhook_api.buffer import BufferFullError, EventBuffer


class FakePublisher:
    def __init__(self) -> None:
        self.batches: list[int] = []
        self.lag = 0

    async def publish_many(self, events) -> int:  # type: ignore[no-untyped-def]
        self.batches.append(len(events))
        return len(events)

    async def pending_lag(self) -> int:
        return self.lag


def _event(index: int) -> EventEnvelope:
    return EventEnvelope(
        event_id=f"evt-{index:08d}",
        event_type="client.upserted",
        occurred_at=datetime.now(UTC),
        partner="pytest",
        data={},
    )


def _settings(**overrides: object) -> Settings:
    return Settings(mssql_password="x", **overrides)  # type: ignore[arg-type]


async def test_agrupa_eventos_em_um_unico_lote() -> None:
    publisher = FakePublisher()
    buffer = EventBuffer(publisher, _settings(buffer_max_batch=500))

    for i in range(120):
        buffer.submit(_event(i))
    await buffer._flush()

    # 120 eventos -> 1 publicacao, nao 120.
    assert publisher.batches == [120]


async def test_respeita_o_tamanho_maximo_do_lote() -> None:
    publisher = FakePublisher()
    buffer = EventBuffer(publisher, _settings(buffer_max_batch=50))

    for i in range(120):
        buffer.submit(_event(i))
    await buffer._flush()
    await buffer._flush()
    await buffer._flush()

    assert publisher.batches == [50, 50, 20]


def test_fila_cheia_sinaliza_em_vez_de_crescer_sem_limite() -> None:
    buffer = EventBuffer(FakePublisher(), _settings(buffer_queue_size=3))

    for i in range(3):
        buffer.submit(_event(i))

    with pytest.raises(BufferFullError):
        buffer.submit(_event(99))
