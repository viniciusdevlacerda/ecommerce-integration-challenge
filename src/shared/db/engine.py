from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.shared.config import Settings

_engines: dict[str, Engine] = {}


def get_engine(settings: Settings) -> Engine:
    url = settings.database_url
    if url not in _engines:
        _engines[url] = _create_bounded_engine(url, settings)
    return _engines[url]


def _create_bounded_engine(url: str, settings: Settings) -> Engine:
    return create_engine(
        url,
        pool_size=settings.db_pool_size,
        # Sem overflow de proposito: o numero de conexoes concorrentes no SQL
        # Server e uma constante que escolhemos, e nao uma funcao do trafego.
        # Sob pico, a carga excedente espera no pool em vez de derrubar o banco.
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle,
        pool_pre_ping=True,
        fast_executemany=True,
        future=True,
    )


def get_session_factory(settings: Settings) -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(settings), expire_on_commit=False, future=True)


@contextmanager
def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
