"""O gerador e o que garante o requisito de memoria da Parte 3."""

from __future__ import annotations

import sys

import pytest

from ecommerce.catalog import generate_cpf
from ecommerce.generator import COLUMNS, generate_rows, plan_chunks


def test_plan_chunks_divide_o_volume_sem_perder_linha() -> None:
    chunks = plan_chunks(10_000_000, 500_000)

    assert len(chunks) == 20
    assert sum(c["row_count"] for c in chunks) == 10_000_000


def test_plan_chunks_trata_resto() -> None:
    chunks = plan_chunks(1_050_000, 500_000)

    assert [c["row_count"] for c in chunks] == [500_000, 500_000, 50_000]


def test_plan_chunks_rejeita_valores_invalidos() -> None:
    with pytest.raises(ValueError):
        plan_chunks(0, 100)


def test_gerador_e_preguicoso_e_nao_materializa_a_base() -> None:
    """Se isto virar uma lista algum dia, 10M de linhas viram varios GB de RAM."""
    rows = generate_rows(chunk_index=0, row_count=10_000_000)

    assert not isinstance(rows, list)
    first = next(rows)
    assert len(first) == len(COLUMNS)
    # Uma linha materializada custa bytes, nao megabytes.
    assert sys.getsizeof(first) < 500


def test_geracao_e_reproduzivel_por_chunk() -> None:
    """Seed derivada do chunk: benchmark comparavel entre execucoes."""
    a = list(generate_rows(chunk_index=7, row_count=5))
    b = list(generate_rows(chunk_index=7, row_count=5))
    c = list(generate_rows(chunk_index=8, row_count=5))

    assert a == b
    assert a != c


def test_chunks_nao_colidem_order_uid() -> None:
    first = {row[0] for row in generate_rows(chunk_index=0, row_count=100)}
    second = {row[0] for row in generate_rows(chunk_index=1, row_count=100)}

    assert not (first & second)


def test_cpf_gerado_tem_digitos_verificadores_validos() -> None:
    import random

    rng = random.Random(1)
    for _ in range(200):
        cpf = generate_cpf(rng)
        digits = [int(d) for d in cpf]

        for position in (9, 10):
            base = digits[:position]
            weight = len(base) + 1
            total = sum(n * (weight - i) for i, n in enumerate(base))
            remainder = (total * 10) % 11
            expected = 0 if remainder == 10 else remainder
            assert digits[position] == expected
