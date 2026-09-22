from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param

from ecommerce.bulk_loader import LoadStrategy, load_csv
from ecommerce.db import execute, scalar
from ecommerce.generator import plan_chunks, write_chunk_csv

logger = logging.getLogger(__name__)

BULK_DIR = os.environ.get("BULK_DIR", "/var/opt/mssql/bulk")
STAGING_TABLE = "stg_order_history"
FACT_TABLE = "fact_order_history"

# Paralelismo suficiente para ganhar tempo, baixo o bastante para que quatro
# BULK INSERT simultaneos nao saturem o SQL Server nem o disco do worker.
MAX_PARALLEL_LOADS = 4
DEFAULT_ROW_COUNT = 10_000_000
DEFAULT_CHUNK_SIZE = 500_000
CHUNK_FILE_PREFIX = "order_history_"

_FACT_COLUMNS = """
    order_uid, client_uid, client_document, order_date, channel,
    payment_method, status, sku, product_name, category, quantity,
    unit_price, freight_amount, total_amount, state, city
"""


@dag(
    dag_id="load_order_history_bulk",
    description="Carga historica de pedidos (10M linhas) com BULK INSERT em chunks",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "bulk-load", "sql-server"],
    params={
        "row_count": Param(DEFAULT_ROW_COUNT, type="integer", minimum=1_000),
        "chunk_size": Param(DEFAULT_CHUNK_SIZE, type="integer", minimum=1_000),
        "strategy": Param(
            LoadStrategy.BULK_INSERT.value,
            type="string",
            enum=[strategy.value for strategy in LoadStrategy],
        ),
    },
    default_args={"retries": 2},
)
def load_order_history_bulk() -> None:
    @task
    def prepare_staging() -> str:
        execute(f"TRUNCATE TABLE {STAGING_TABLE}")
        os.makedirs(BULK_DIR, exist_ok=True)
        return STAGING_TABLE

    @task
    def plan(**context: Any) -> list[dict[str, int]]:
        params = context["params"]
        chunks = plan_chunks(int(params["row_count"]), int(params["chunk_size"]))
        logger.info(
            "plano de carga",
            extra={"total_rows": params["row_count"], "chunks": len(chunks)},
        )
        return chunks

    @task(max_active_tis_per_dag=MAX_PARALLEL_LOADS, retries=2)
    def generate_and_load(chunk: dict[str, int], **context: Any) -> dict[str, Any]:
        strategy = LoadStrategy(context["params"]["strategy"])

        path = write_chunk_csv(
            chunk_index=chunk["chunk_index"],
            row_count=chunk["row_count"],
            target_dir=BULK_DIR,
        )
        load_csv(path=path, table=STAGING_TABLE, strategy=strategy)
        os.remove(path)

        return {"chunk_index": chunk["chunk_index"], "rows": chunk["row_count"]}

    @task
    def load_fact(loaded_chunks: list[dict[str, Any]]) -> int:
        execute(f"TRUNCATE TABLE {FACT_TABLE}")
        execute(
            f"""
            INSERT INTO {FACT_TABLE} WITH (TABLOCK) ({_FACT_COLUMNS})
            SELECT {_FACT_COLUMNS} FROM {STAGING_TABLE}
            """  # noqa: S608
        )
        total = _count_fact_rows()
        logger.info("fato carregado", extra={"rows": total, "chunks": len(loaded_chunks)})
        return total

    @task
    def validate_load(fact_rows: int, **context: Any) -> dict[str, Any]:
        expected = int(context["params"]["row_count"])
        if fact_rows != expected:
            raise ValueError(f"esperado {expected} linhas, encontrado {fact_rows}")

        incomplete = _count_incomplete_rows()
        if incomplete:
            raise ValueError(f"{incomplete} linhas com campos obrigatorios nulos")

        execute(f"UPDATE STATISTICS {FACT_TABLE}")
        report = {
            "rows": fact_rows,
            "expected": expected,
            "distinct_skus": _count_distinct_skus(),
        }
        logger.info("validacao ok", extra=report)
        return report

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        if not os.path.isdir(BULK_DIR):
            return
        for name in os.listdir(BULK_DIR):
            if name.startswith(CHUNK_FILE_PREFIX) and name.endswith(".csv"):
                os.remove(os.path.join(BULK_DIR, name))

    staging = prepare_staging()
    chunks = plan()
    staging >> chunks

    loaded = generate_and_load.expand(chunk=chunks)
    report = validate_load(load_fact(loaded))
    report >> cleanup()


def _count_fact_rows() -> int:
    return int(scalar(f"SELECT COUNT_BIG(1) FROM {FACT_TABLE}") or 0)  # noqa: S608


def _count_distinct_skus() -> int:
    return int(scalar(f"SELECT COUNT(DISTINCT sku) FROM {FACT_TABLE}") or 0)  # noqa: S608


def _count_incomplete_rows() -> int:
    statement = f"""
        SELECT COUNT_BIG(1) FROM {FACT_TABLE}
         WHERE order_uid IS NULL OR total_amount IS NULL OR order_date IS NULL
    """  # noqa: S608
    return int(scalar(statement) or 0)


load_order_history_bulk()
