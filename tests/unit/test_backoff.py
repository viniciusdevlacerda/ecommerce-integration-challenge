"""Backoff do dispatch-worker: cresce, tem teto e tem jitter."""

from __future__ import annotations

from src.dispatch_worker.erp_client import backoff_delay
from src.shared.config import Settings


def _settings() -> Settings:
    return Settings(
        mssql_password="x",
        dispatch_backoff_base_seconds=1.0,
        dispatch_backoff_max_seconds=60.0,
    )


def test_espera_cresce_com_a_tentativa() -> None:
    settings = _settings()
    early = max(backoff_delay(1, settings) for _ in range(50))
    late = max(backoff_delay(6, settings) for _ in range(50))

    assert late > early


def test_respeita_o_teto() -> None:
    settings = _settings()
    assert all(backoff_delay(20, settings) <= 60.0 for _ in range(100))


def test_tem_jitter() -> None:
    """Sem jitter, todas as mensagens voltam juntas e derrubam o ERP de novo."""
    settings = _settings()
    values = {round(backoff_delay(4, settings), 6) for _ in range(50)}

    assert len(values) > 1
