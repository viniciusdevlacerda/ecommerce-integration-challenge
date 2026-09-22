from __future__ import annotations

import csv
import logging
from enum import StrEnum
from typing import Any

from sqlalchemy import text

from ecommerce.db import connection
from ecommerce.generator import COLUMNS

logger = logging.getLogger(__name__)

EXECUTEMANY_BATCH_SIZE = 50_000
BULK_INSERT_BATCH_SIZE = 100_000


class LoadStrategy(StrEnum):
    BULK_INSERT = "bulk_insert"
    FAST_EXECUTEMANY = "fast_executemany"


# TABLOCK habilita minimal logging; o arquivo e lido pelo processo do SQL Server,
# nao pelo driver Python.
_BULK_INSERT_TEMPLATE = """
BULK INSERT {table}
FROM '{path}'
WITH (
    FORMAT          = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '0x0a',
    FIRSTROW        = 2,
    BATCHSIZE       = {batch_size},
    TABLOCK,
    MAXERRORS       = 0
)
"""


def load_csv(*, path: str, table: str, strategy: LoadStrategy) -> int:
    if strategy is LoadStrategy.BULK_INSERT:
        return _load_with_bulk_insert(path=path, table=table)
    return _load_with_fast_executemany(path=path, table=table)


def _load_with_bulk_insert(*, path: str, table: str) -> int:
    statement = _BULK_INSERT_TEMPLATE.format(
        table=table, path=path, batch_size=BULK_INSERT_BATCH_SIZE
    )
    with connection() as conn:
        conn.execute(text(statement))
        loaded = conn.execute(
            text(f"SELECT COUNT_BIG(1) FROM {table} WITH (NOLOCK)")  # noqa: S608
        ).scalar()

    logger.info("BULK INSERT concluido", extra={"path": path, "table_rows": loaded})
    return int(loaded or 0)


def _load_with_fast_executemany(*, path: str, table: str) -> int:
    statement = _insert_statement(table)
    total = 0

    with connection() as conn:
        cursor = _raw_cursor(conn)
        cursor.fast_executemany = True  # type: ignore[attr-defined]

        for batch in _read_in_batches(path, EXECUTEMANY_BATCH_SIZE):
            cursor.executemany(statement, batch)  # type: ignore[attr-defined]
            total += len(batch)

        cursor.close()  # type: ignore[attr-defined]

    logger.info("fast_executemany concluido", extra={"path": path, "rows": total})
    return total


def _read_in_batches(path: str, batch_size: int) -> Any:
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        next(reader, None)

        batch: list[list[str]] = []
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch


def _insert_statement(table: str) -> str:
    placeholders = ", ".join(["?"] * len(COLUMNS))
    return f"INSERT INTO {table} ({', '.join(COLUMNS)}) VALUES ({placeholders})"  # noqa: S608


def _raw_cursor(conn: object) -> object:
    """Cursor DBAPI cru: o Airflow roda SQLAlchemy 1.4 e a aplicacao roda 2.0."""
    raw = conn.connection  # type: ignore[attr-defined]
    dbapi = getattr(raw, "dbapi_connection", None) or getattr(raw, "connection", raw)
    return dbapi.cursor()
