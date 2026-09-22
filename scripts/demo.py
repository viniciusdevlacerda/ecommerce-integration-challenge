from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import text

from src.shared.config import get_settings
from src.shared.db.engine import get_engine

API = "http://localhost:8000"
ERP = "http://erp-mock:8000"
PARTNER = "partner-checkout"

settings = get_settings()
engine = get_engine(settings)

RUN = uuid.uuid4().hex[:8]
CLIENT_UID = f"DEMO-CLI-{RUN}"
ADDRESS_UID = f"DEMO-ADDR-{RUN}"
ORDER_UID = f"DEMO-ORD-{RUN}"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _event(event_type: str, data: dict[str, Any], event_id: str | None = None) -> dict[str, Any]:
    return {
        "event_id": event_id or f"evt-{uuid.uuid4().hex}",
        "event_type": event_type,
        "occurred_at": _now(),
        "partner": PARTNER,
        "data": data,
    }


def _post(events: list[dict[str, Any]]) -> httpx.Response:
    return httpx.post(f"{API}/webhooks/{PARTNER}/events", json=events, timeout=15)


def _sql(query: str, **params: Any) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(query), params).scalar()


def _wait(
    description: str, query: str, expected: int, timeout: float = 45.0, **params: Any
) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if int(_sql(query, **params) or 0) >= expected:
            print(f"   ok   {description}")
            return True
        time.sleep(0.5)
    print(f"   FALHA {description} (timeout)")
    return False


def _header(title: str) -> None:
    print(f"\n{'=' * 74}\n {title}\n{'=' * 74}")


def step_ingest() -> None:
    _header("1. INGESTAO  webhook -> Redis Stream -> SQL Server")

    address = {
        "address_uid": ADDRESS_UID,
        "street": "Avenida das Industrias",
        "number": "1500",
        "complement": "Galpao 3",
        "district": "Distrito Industrial",
        "city": "Joinville",
        "state": "SC",
        "zip_code": "89219000",
    }
    client_event = _event(
        "client.upserted",
        {
            "client_uid": CLIENT_UID,
            "full_name": "Mariana Ferreira Souza",
            "document": "39053344705",
            "email": f"mariana.{RUN}@exemplo.com.br",
            "address": address,
        },
    )
    response = _post([client_event])
    print(f"   POST cliente  -> HTTP {response.status_code} {response.json()}")
    _wait(
        "cliente persistido",
        "SELECT COUNT(1) FROM clients WHERE client_uid = :uid",
        1,
        uid=CLIENT_UID,
    )

    order_event = _event(
        "order.created",
        {
            "order_uid": ORDER_UID,
            "client_uid": CLIENT_UID,
            "shipping_address_uid": ADDRESS_UID,
            "currency": "BRL",
            "placed_at": _now(),
            "items": [
                {"sku": "SKU-AF-8000", "description": "Air Fryer Familia 8L",
                 "quantity": 1, "unit_price": "649.90"},
                {"sku": "SKU-LQ-1200", "description": "Liquidificador 1200W 12 Velocidades",
                 "quantity": 2, "unit_price": "219.90"},
            ],
        },
    )
    response = _post([order_event])
    print(f"   POST pedido   -> HTTP {response.status_code} {response.json()}")
    _wait(
        "pedido persistido",
        "SELECT COUNT(1) FROM orders WHERE order_uid = :uid",
        1,
        uid=ORDER_UID,
    )

    globals()["ORDER_EVENT"] = order_event


def step_idempotency() -> None:
    _header("2. IDEMPOTENCIA  duplicatas nao geram pedido nem faturamento duplicado")

    order_event: dict[str, Any] = globals()["ORDER_EVENT"]

    # Camadas 1 e 2: mesmo event_id.
    _post([order_event, order_event, order_event])
    print("   reenviado 3x o MESMO event_id")

    # Camada 3: event_id novo, mesmo pedido. E este o caso que pega quem
    # deduplica apenas por chave de evento.
    reissued = dict(order_event)
    reissued["event_id"] = f"evt-{uuid.uuid4().hex}"
    _post([reissued])
    print("   reenviado com event_id NOVO e mesmo order_uid")

    time.sleep(4)
    orders = int(_sql("SELECT COUNT(1) FROM orders WHERE order_uid = :uid", uid=ORDER_UID) or 0)
    items = int(
        _sql(
            "SELECT COUNT(1) FROM order_items i JOIN orders o ON o.order_id = i.order_id "
            "WHERE o.order_uid = :uid",
            uid=ORDER_UID,
        )
        or 0
    )
    print(f"\n   pedidos com esse order_uid ..... {orders}  (esperado: 1)")
    print(f"   itens do pedido ................ {items}  (esperado: 2)")
    print(f"   {'PASSOU' if orders == 1 and items == 2 else 'FALHOU'}")


def step_payment_and_erp() -> None:
    _header("3. EVENT-DRIVEN  pagamento aprovado -> outbox -> ERP (com falhas injetadas)")

    payment = _event(
        "payment.updated",
        {
            "order_uid": ORDER_UID,
            "provider": "pagarme",
            "provider_tx_id": f"tx-{RUN}",
            "method": "PIX",
            "status": "APPROVED",
            "amount": "1089.70",
            "occurred_at": _now(),
        },
    )
    _post([payment, payment])  # duplicado de proposito
    print("   POST pagamento APROVADO (enviado 2x de proposito)")

    _wait(
        "pedido em APPROVED",
        "SELECT COUNT(1) FROM orders WHERE order_uid = :uid AND status = 'APPROVED'",
        1,
        uid=ORDER_UID,
    )
    outbox = int(
        _sql("SELECT COUNT(1) FROM outbox WHERE aggregate_id = :uid", uid=ORDER_UID) or 0
    )
    print(
        f"   mensagens na outbox ............ {outbox}"
        "  (esperado: 1 -- a duplicata nao gerou uma segunda)"
    )

    print("\n   aguardando o dispatch-worker vencer as falhas injetadas do ERP...")
    sent = _wait(
        "mensagem entregue ao ERP (status SENT)",
        "SELECT COUNT(1) FROM outbox WHERE aggregate_id = :uid AND status = 'SENT'",
        1,
        timeout=90,
        uid=ORDER_UID,
    )

    attempts = int(
        _sql("SELECT MAX(attempts) FROM outbox WHERE aggregate_id = :uid", uid=ORDER_UID) or 0
    )
    print(f"   tentativas ate entregar ........ {attempts}")

    if sent:
        erp = httpx.get(f"{ERP}/erp/orders", timeout=10).json()
        mine = [o for o in erp["orders"] if o["order_uid"] == ORDER_UID]
        print(f"   ERP confirmou .................. {len(mine)} pedido(s): "
              f"{mine[0]['erp_order_id'] if mine else '-'}")
        print(f"   taxa de falha injetada ......... {erp['failure_rate']:.0%}")


def step_immutability() -> None:
    _header("4. IMUTABILIDADE  pedido fechado nao aceita alteracao retroativa")

    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE orders SET total_amount = 1.00 WHERE order_uid = :uid"),
                {"uid": ORDER_UID},
            )
        print("   FALHOU  o UPDATE passou -- o trigger nao esta ativo")
    except Exception as exc:
        message = str(getattr(exc, "orig", exc))
        detail = message[message.find("Pedido") :][:80] or message[:80]
        print(f"   PASSOU  banco recusou o UPDATE: {detail}")


def step_scd2() -> None:
    _header("5. SCD TIPO 2  cliente muda de endereco, pedido antigo nao muda")

    before = _sql(
        "SELECT a.city FROM orders o JOIN address a ON a.address_id = o.shipping_address_id "
        "WHERE o.order_uid = :uid",
        uid=ORDER_UID,
    )

    moved = _event(
        "client.upserted",
        {
            "client_uid": CLIENT_UID,
            "full_name": "Mariana Ferreira Souza",
            "document": "39053344705",
            "email": f"mariana.{RUN}@exemplo.com.br",
            "address": {
                "address_uid": ADDRESS_UID,
                "street": "Rua Nova do Comercio",
                "number": "42",
                "complement": None,
                "district": "Centro",
                "city": "Curitiba",
                "state": "PR",
                "zip_code": "80010000",
            },
        },
    )
    _post([moved])
    print("   cliente se mudou de Joinville/SC para Curitiba/PR")
    time.sleep(4)

    versions = int(
        _sql("SELECT COUNT(1) FROM address WHERE address_uid = :uid", uid=ADDRESS_UID) or 0
    )
    current = _sql(
        "SELECT city FROM address WHERE address_uid = :uid AND is_current = 1", uid=ADDRESS_UID
    )
    order_city = _sql(
        "SELECT a.city FROM orders o JOIN address a ON a.address_id = o.shipping_address_id "
        "WHERE o.order_uid = :uid",
        uid=ORDER_UID,
    )

    print(f"   versoes do endereco ............ {versions}  (esperado: 2)")
    print(f"   endereco vigente do cliente .... {current}")
    print(f"   endereco DO PEDIDO ............. {order_city}  (antes: {before})")
    print(f"   {'PASSOU' if order_city == before and versions == 2 else 'FALHOU'}")


def main() -> None:
    print(f"\nDEMO ponta a ponta | execucao {RUN}")
    step_ingest()
    step_idempotency()
    step_payment_and_erp()
    step_immutability()
    step_scd2()
    print(f"\n{'=' * 74}\n Demo concluida. Airflow: http://localhost:8080 (admin/admin)")
    print(" Resumo do estado do banco: ./run.sh verify\n")


if __name__ == "__main__":
    main()
