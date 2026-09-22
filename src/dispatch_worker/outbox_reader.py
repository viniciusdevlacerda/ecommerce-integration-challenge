from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

STALE_LOCK_SECONDS = 120

# READPAST faz workers concorrentes pularem linhas ja travadas em vez de esperar.
_CLAIM_SKIPPING_LOCKED_ROWS = text(
    """
    UPDATE TOP (:batch_size) outbox WITH (ROWLOCK, UPDLOCK, READPAST)
       SET status      = 'IN_FLIGHT',
           attempts    = attempts + 1,
           locked_by   = :worker_id,
           locked_at   = SYSUTCDATETIME()
    OUTPUT inserted.outbox_id,
           inserted.aggregate_id,
           inserted.event_type,
           inserted.payload,
           inserted.attempts
     WHERE status = 'PENDING'
       AND next_attempt_at <= SYSUTCDATETIME()
    """
)

_RELEASE_STALE_LOCKS = text(
    """
    UPDATE outbox
       SET status = 'PENDING', locked_by = NULL, locked_at = NULL
     WHERE status = 'IN_FLIGHT'
       AND locked_at < DATEADD(SECOND, -:stale_seconds, SYSUTCDATETIME())
    """
)


@dataclass(frozen=True, slots=True)
class OutboxItem:
    outbox_id: int
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    attempts: int


class OutboxReader:
    def __init__(self, worker_id: str) -> None:
        self._worker_id = worker_id

    def claim(self, session: Session, batch_size: int) -> list[OutboxItem]:
        rows = session.execute(
            _CLAIM_SKIPPING_LOCKED_ROWS,
            {"batch_size": batch_size, "worker_id": self._worker_id},
        ).all()
        session.commit()
        return [_to_item(row) for row in rows]

    @staticmethod
    def release_abandoned(session: Session, stale_seconds: int = STALE_LOCK_SECONDS) -> int:
        result = session.execute(_RELEASE_STALE_LOCKS, {"stale_seconds": stale_seconds})
        session.commit()
        return int(result.rowcount or 0)


def _to_item(row: Any) -> OutboxItem:
    return OutboxItem(
        outbox_id=row.outbox_id,
        aggregate_id=row.aggregate_id,
        event_type=row.event_type,
        payload=json.loads(row.payload),
        attempts=row.attempts,
    )


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
