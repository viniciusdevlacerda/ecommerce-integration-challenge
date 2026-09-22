from __future__ import annotations

import csv
import os
import random
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ecommerce.catalog import (
    CHANNELS,
    LOCATIONS,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PRODUCTS,
    generate_cpf,
)

COLUMNS: tuple[str, ...] = (
    "order_uid", "client_uid", "client_document", "order_date", "channel",
    "payment_method", "status", "sku", "product_name", "category", "quantity",
    "unit_price", "freight_amount", "total_amount", "state", "city",
)

# O default do modulo csv e "\r\n", que quebraria o ROWTERMINATOR='0x0a' do BULK INSERT.
CSV_LINE_TERMINATOR = "\n"
CSV_WRITE_BUFFER_BYTES = 1024 * 1024
FILE_MODE_READABLE_BY_SQL_SERVER = 0o644

CPF_POOL_SIZE = 5_000
CLIENT_UNIVERSE = 900_000
MAX_ITEMS_PER_ORDER = 4
MAX_FREIGHT = 89.90
STATUS_WEIGHTS = (70, 20, 7, 3)

FIRST_ORDER_DATE = date(2023, 1, 1)
ORDER_DATE_SPAN_DAYS = 1_000
CHUNK_SEED_OFFSET = 1_000


def generate_rows(chunk_index: int, row_count: int) -> Iterator[tuple[Any, ...]]:
    """Gera as linhas uma a uma, sem nunca materializar a base.

    E o que garante o requisito de memoria: o pico e o mesmo para mil ou para
    dez milhoes de linhas. Montar lista ou DataFrame aqui custaria varios GB.

    A seed deriva do indice do chunk, entao a base e reproduzivel entre
    execucoes e o benchmark fica comparavel.
    """
    rng = random.Random(CHUNK_SEED_OFFSET + chunk_index)
    first_sequence = chunk_index * row_count
    cpf_pool = [generate_cpf(rng) for _ in range(CPF_POOL_SIZE)]

    for offset in range(row_count):
        sequence = first_sequence + offset
        product = rng.choice(PRODUCTS)
        state, city, _zip_prefix = rng.choice(LOCATIONS)

        quantity = rng.randint(1, MAX_ITEMS_PER_ORDER)
        unit_price = round(rng.uniform(product.min_price, product.max_price), 2)
        freight = round(rng.uniform(0.0, MAX_FREIGHT), 2)

        yield (
            f"HIST-{sequence:012d}",
            f"CLI-{rng.randint(1, CLIENT_UNIVERSE):08d}",
            cpf_pool[sequence % CPF_POOL_SIZE],
            _random_order_date(rng).isoformat(),
            rng.choice(CHANNELS),
            rng.choice(PAYMENT_METHODS),
            rng.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0],
            product.sku,
            product.name,
            product.category,
            quantity,
            unit_price,
            freight,
            round(unit_price * quantity + freight, 2),
            state,
            city,
        )


def write_chunk_csv(*, chunk_index: int, row_count: int, target_dir: str) -> str:
    path = _chunk_path(target_dir, chunk_index)

    with path.open(
        "w", newline="", encoding="utf-8", buffering=CSV_WRITE_BUFFER_BYTES
    ) as handle:
        writer = csv.writer(
            handle,
            delimiter=",",
            quoting=csv.QUOTE_MINIMAL,
            lineterminator=CSV_LINE_TERMINATOR,
        )
        writer.writerow(COLUMNS)
        writer.writerows(generate_rows(chunk_index, row_count))

    os.chmod(path, FILE_MODE_READABLE_BY_SQL_SERVER)
    return str(path)


def plan_chunks(row_count: int, chunk_size: int) -> list[dict[str, int]]:
    if row_count <= 0 or chunk_size <= 0:
        raise ValueError("row_count e chunk_size precisam ser positivos")

    chunks: list[dict[str, int]] = []
    remaining = row_count
    while remaining > 0:
        size = min(chunk_size, remaining)
        chunks.append({"chunk_index": len(chunks), "row_count": size})
        remaining -= size
    return chunks


def _random_order_date(rng: random.Random) -> date:
    return FIRST_ORDER_DATE + timedelta(days=rng.randint(0, ORDER_DATE_SPAN_DAYS))


def _chunk_path(target_dir: str, chunk_index: int) -> Path:
    directory = Path(target_dir)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"order_history_{chunk_index:04d}.csv"
