from __future__ import annotations

import csv
import os
import resource
import time
from dataclasses import dataclass

from sqlalchemy import text

from ecommerce.bulk_loader import LoadStrategy, _raw_cursor, load_csv
from ecommerce.db import connection, execute
from ecommerce.generator import COLUMNS, write_chunk_csv

BULK_DIR = os.environ.get("BULK_DIR", "/var/opt/mssql/bulk")
TABLE = "stg_order_history"

SAMPLE_FAST = 200_000   # BULK INSERT e fast_executemany
SAMPLE_ROW = 5_000      # linha a linha -- so para estabelecer a ordem de grandeza


@dataclass(frozen=True, slots=True)
class Measurement:
    label: str
    rows: int
    seconds: float
    peak_memory_mb: float

    @property
    def rows_per_second(self) -> float:
        return self.rows / self.seconds if self.seconds else 0.0


def _peak_memory_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage / 1024  # Linux reporta em KB


def _reset() -> None:
    execute(f"TRUNCATE TABLE {TABLE}")


def _measure(label: str, rows: int, fn: object) -> Measurement:
    _reset()
    start = time.perf_counter()
    fn()  # type: ignore[operator]
    elapsed = time.perf_counter() - start
    return Measurement(label, rows, elapsed, _peak_memory_mb())


def _row_by_row(path: str, limit: int) -> None:
    """O anti-padrao, para efeito de comparacao: um round-trip por linha."""
    placeholders = ", ".join(["?"] * len(COLUMNS))
    sql = f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES ({placeholders})"  # noqa: S608
    with connection() as conn:
        cursor = _raw_cursor(conn)
        with open(path, newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            for index, row in enumerate(reader):
                if index >= limit:
                    break
                cursor.execute(sql, row)
        cursor.close()


def main() -> None:
    print("gerando amostras...", flush=True)
    big = write_chunk_csv(chunk_index=900, row_count=SAMPLE_FAST, target_dir=BULK_DIR)

    results = [
        _measure(
            "BULK INSERT (volume compartilhado)",
            SAMPLE_FAST,
            lambda: load_csv(path=big, table=TABLE, strategy=LoadStrategy.BULK_INSERT),
        ),
        _measure(
            "pyodbc fast_executemany",
            SAMPLE_FAST,
            lambda: load_csv(path=big, table=TABLE, strategy=LoadStrategy.FAST_EXECUTEMANY),
        ),
        _measure(
            "INSERT linha a linha",
            SAMPLE_ROW,
            lambda: _row_by_row(big, SAMPLE_ROW),
        ),
    ]

    baseline = results[0].rows_per_second
    width = 38
    print()
    head = f"{'estrategia':<{width}} {'linhas':>10} {'segundos':>10}"
    print(f"{head} {'linhas/s':>12} {'10M em':>12}")
    print("-" * (width + 48))
    for r in results:
        projected = 10_000_000 / r.rows_per_second if r.rows_per_second else float("inf")
        human = f"{projected / 60:.1f} min" if projected < 36_000 else f"{projected / 3600:.1f} h"
        print(
            f"{r.label:<{width}} {r.rows:>10,} {r.seconds:>10.2f} "
            f"{r.rows_per_second:>12,.0f} {human:>12}"
        )
    print("-" * (width + 48))
    print(f"pico de memoria do processo: {results[-1].peak_memory_mb:.0f} MB")
    print(f"BULK INSERT e a referencia ({baseline:,.0f} linhas/s).")
    print("A projecao de 10M e extrapolacao linear -- a medicao real usa a DAG.")

    _reset()
    with connection() as conn:
        conn.execute(text("SELECT 1"))
    if os.path.exists(big):
        os.remove(big)


if __name__ == "__main__":
    main()
