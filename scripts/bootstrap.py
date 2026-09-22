from __future__ import annotations

import logging
import sys
import time

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from src.shared.config import get_settings
from src.shared.logging import configure_logging

logger = logging.getLogger(__name__)


def wait_for_server(master_url: str, attempts: int = 30, delay: float = 2.0) -> None:
    for attempt in range(1, attempts + 1):
        try:
            engine = create_engine(master_url, isolation_level="AUTOCOMMIT")
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except OperationalError as exc:
            logger.warning(
                "SQL Server ainda nao respondeu",
                extra={"attempt": attempt, "error": str(exc)[:120]},
            )
            time.sleep(delay)
    raise SystemExit("SQL Server nao ficou disponivel a tempo")


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    wait_for_server(settings.master_url)
    engine = create_engine(settings.master_url, isolation_level="AUTOCOMMIT")
    database = settings.mssql_db

    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT DB_ID(:name)"), {"name": database}
        ).scalar()
        if exists is None:
            conn.execute(text(f"CREATE DATABASE [{database}]"))
            logger.info("database criado", extra={"database": database})

        rcsi = f"ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE"
        conn.execute(text(rcsi))
        conn.execute(text(f"ALTER DATABASE [{database}] SET ALLOW_SNAPSHOT_ISOLATION ON"))
        conn.execute(text(f"ALTER DATABASE [{database}] SET RECOVERY SIMPLE"))

    logger.info("bootstrap concluido", extra={"database": database})


if __name__ == "__main__":
    sys.exit(main())
