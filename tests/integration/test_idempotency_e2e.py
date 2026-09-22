"""Idempotencia ponta a ponta, com a stack no ar.

Roda com:  docker compose exec webhook-api pytest -m integration
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from sqlalchemy import text

from src.shared.config import get_settings
from src.shared.db.engine import get_engine

pytestmark = pytest.mark.integration

API = "http://localhost:8000"
PARTNER = "pytest"

engine = get_engine(get_settings())


def _post(events: list[dict[str, Any]]) -> httpx.Response:
    return httpx.post(f"{API}/webhooks/{PARTNER}/events", json=events, timeout=15)


def _count(query: str, **params: Any) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(query), params).scalar() or 0)


def _wait_for(query: str, expected: int, timeout: float = 40.0, **params: Any) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _count(query, **params) >= expected:
            return True
        time.sleep(0.5)
    return False


def _event(event_type: str, data: dict[str, Any], event_id: str | None = None) -> dict[str, Any]:
    return {
        "event_id": event_id or f"evt-{uuid.uuid4().hex}",
        "event_type": event_type,
        "occurred_at": datetime.now(UTC).isoformat(),
        "partner": PARTNER,
        "data": data,
    }


@pytest.fixture
def seeded_client() -> tuple[str, str]:
    run = uuid.uuid4().hex[:8]
    client_uid, address_uid = f"IT-CLI-{run}", f"IT-ADDR-{run}"

    _post(
        [
            _event(
                "client.upserted",
                {
                    "client_uid": client_uid,
                    "full_name": "Cliente Integracao",
                    "document": "39053344705",
                    "email": f"it.{run}@exemplo.com.br",
                    "address": {
                        "address_uid": address_uid,
                        "street": "Avenida das Industrias",
                        "number": "1500",
                        "complement": None,
                        "district": "Distrito Industrial",
                        "city": "Joinville",
                        "state": "SC",
                        "zip_code": "89219000",
                    },
                },
            )
        ]
    )
    assert _wait_for(
        "SELECT COUNT(1) FROM clients WHERE client_uid = :uid", 1, uid=client_uid
    ), "cliente nao foi ingerido"
    return client_uid, address_uid


def _order_event(order_uid: str, client_uid: str, address_uid: str) -> dict[str, Any]:
    return _event(
        "order.created",
        {
            "order_uid": order_uid,
            "client_uid": client_uid,
            "shipping_address_uid": address_uid,
            "currency": "BRL",
            "placed_at": datetime.now(UTC).isoformat(),
            "items": [
                {"sku": "SKU-AF-4000", "description": "Air Fryer Digital 4L",
                 "quantity": 1, "unit_price": "449.90"}
            ],
        },
    )


def test_mesmo_event_id_reenviado_nao_duplica_pedido(seeded_client: tuple[str, str]) -> None:
    client_uid, address_uid = seeded_client
    order_uid = f"IT-ORD-{uuid.uuid4().hex[:8]}"
    event = _order_event(order_uid, client_uid, address_uid)

    _post([event] * 5)

    assert _wait_for("SELECT COUNT(1) FROM orders WHERE order_uid = :uid", 1, uid=order_uid)
    time.sleep(3)
    assert _count("SELECT COUNT(1) FROM orders WHERE order_uid = :uid", uid=order_uid) == 1


def test_event_id_novo_com_mesmo_pedido_tambem_nao_duplica(
    seeded_client: tuple[str, str],
) -> None:
    """O caso que derruba quem so deduplica por chave de evento."""
    client_uid, address_uid = seeded_client
    order_uid = f"IT-ORD-{uuid.uuid4().hex[:8]}"

    first = _order_event(order_uid, client_uid, address_uid)
    second = dict(first)
    second["event_id"] = f"evt-{uuid.uuid4().hex}"

    _post([first])
    assert _wait_for("SELECT COUNT(1) FROM orders WHERE order_uid = :uid", 1, uid=order_uid)
    _post([second])
    time.sleep(3)

    assert _count("SELECT COUNT(1) FROM orders WHERE order_uid = :uid", uid=order_uid) == 1


def test_pagamento_duplicado_nao_gera_duas_mensagens_de_outbox(
    seeded_client: tuple[str, str],
) -> None:
    client_uid, address_uid = seeded_client
    order_uid = f"IT-ORD-{uuid.uuid4().hex[:8]}"

    _post([_order_event(order_uid, client_uid, address_uid)])
    assert _wait_for("SELECT COUNT(1) FROM orders WHERE order_uid = :uid", 1, uid=order_uid)

    payment = _event(
        "payment.updated",
        {
            "order_uid": order_uid,
            "provider": "pagarme",
            "provider_tx_id": f"tx-{uuid.uuid4().hex[:10]}",
            "method": "PIX",
            "status": "APPROVED",
            "amount": "449.90",
            "occurred_at": datetime.now(UTC).isoformat(),
        },
    )
    reissued = dict(payment)
    reissued["event_id"] = f"evt-{uuid.uuid4().hex}"

    _post([payment, payment, reissued])

    assert _wait_for(
        "SELECT COUNT(1) FROM orders WHERE order_uid = :uid AND status = 'APPROVED'",
        1,
        uid=order_uid,
    )
    time.sleep(3)

    assert _count("SELECT COUNT(1) FROM outbox WHERE aggregate_id = :uid", uid=order_uid) == 1
    assert _count(
        "SELECT COUNT(1) FROM payment p JOIN orders o ON o.order_id = p.order_id "
        "WHERE o.order_uid = :uid",
        uid=order_uid,
    ) == 1


def test_pedido_fechado_rejeita_alteracao_retroativa(seeded_client: tuple[str, str]) -> None:
    client_uid, address_uid = seeded_client
    order_uid = f"IT-ORD-{uuid.uuid4().hex[:8]}"

    _post([_order_event(order_uid, client_uid, address_uid)])
    assert _wait_for("SELECT COUNT(1) FROM orders WHERE order_uid = :uid", 1, uid=order_uid)

    _post(
        [
            _event(
                "payment.updated",
                {
                    "order_uid": order_uid,
                    "provider": "pagarme",
                    "provider_tx_id": f"tx-{uuid.uuid4().hex[:10]}",
                    "method": "BOLETO",
                    "status": "APPROVED",
                    "amount": "449.90",
                    "occurred_at": datetime.now(UTC).isoformat(),
                },
            )
        ]
    )
    assert _wait_for(
        "SELECT COUNT(1) FROM orders WHERE order_uid = :uid AND status = 'APPROVED'",
        1,
        uid=order_uid,
    )

    with pytest.raises(Exception, match="(?i)imutavel|immutable|50001"):
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE orders SET total_amount = 1 WHERE order_uid = :uid"),
                {"uid": order_uid},
            )


def test_endereco_versiona_e_pedido_mantem_a_versao_antiga(
    seeded_client: tuple[str, str],
) -> None:
    client_uid, address_uid = seeded_client
    order_uid = f"IT-ORD-{uuid.uuid4().hex[:8]}"

    _post([_order_event(order_uid, client_uid, address_uid)])
    assert _wait_for("SELECT COUNT(1) FROM orders WHERE order_uid = :uid", 1, uid=order_uid)

    original_address_id = _count(
        "SELECT shipping_address_id FROM orders WHERE order_uid = :uid", uid=order_uid
    )

    _post(
        [
            _event(
                "client.upserted",
                {
                    "client_uid": client_uid,
                    "full_name": "Cliente Integracao",
                    "document": "39053344705",
                    "email": "it@exemplo.com.br",
                    "address": {
                        "address_uid": address_uid,
                        "street": "Rua Nova",
                        "number": "42",
                        "complement": None,
                        "district": "Centro",
                        "city": "Curitiba",
                        "state": "PR",
                        "zip_code": "80010000",
                    },
                },
            )
        ]
    )
    assert _wait_for(
        "SELECT COUNT(1) FROM address WHERE address_uid = :uid", 2, uid=address_uid
    )

    # O pedido continua apontando para a versao que existia no fechamento.
    assert _count(
        "SELECT shipping_address_id FROM orders WHERE order_uid = :uid", uid=order_uid
    ) == original_address_id
    assert _count(
        "SELECT COUNT(1) FROM address WHERE address_uid = :uid AND is_current = 1",
        uid=address_uid,
    ) == 1
