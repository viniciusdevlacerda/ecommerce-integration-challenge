from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import quote_plus, urlencode

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

_engine: Engine | None = None


def database_url(database: str | None = None) -> str:
    query = urlencode(
        {
            "driver": os.environ.get("ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
            "TrustServerCertificate": "yes",
            "Encrypt": "yes",
        }
    )
    user = os.environ.get("MSSQL_USER", "sa")
    password = quote_plus(os.environ["MSSQL_PASSWORD"])
    host = os.environ.get("MSSQL_HOST", "mssql")
    port = os.environ.get("MSSQL_PORT", "1433")
    name = database or os.environ.get("MSSQL_DB", "cliente")
    return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{name}?{query}"


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            database_url(), pool_pre_ping=True, isolation_level="AUTOCOMMIT", future=True
        )
    return _engine


@contextmanager
def connection() -> Iterator[Connection]:
    with get_engine().connect() as conn:
        yield conn


def execute(sql: str, **params: object) -> None:
    with connection() as conn:
        conn.execute(text(sql), params)


def scalar(sql: str, **params: object) -> object:
    with connection() as conn:
        return conn.execute(text(sql), params).scalar()
