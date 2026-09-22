from __future__ import annotations

import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "airflow" / "dags"))


@pytest.fixture
def make_event() -> Any:
    def _factory(
        event_type: str, data: dict[str, Any], event_id: str | None = None
    ) -> dict[str, Any]:
        return {
            "event_id": event_id or f"evt-{uuid.uuid4().hex}",
            "event_type": event_type,
            "occurred_at": datetime.now(UTC).isoformat(),
            "partner": "pytest",
            "data": data,
        }

    return _factory
