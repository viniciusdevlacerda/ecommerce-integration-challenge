from __future__ import annotations

import asyncio
import random
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel

from src.shared.config import get_settings
from src.shared.logging import configure_logging

settings = get_settings()
configure_logging(settings.log_level)

app = FastAPI(
    title="ERP ficticio",
    version="1.0.0",
    description="Simula o ERP de destino, com injecao de falhas e idempotencia por chave.",
)

SERVER_ERROR_CODES = (500, 503)
TIMEOUT_SHARE_OF_FAILURES = 0.5

_acknowledged_by_key: dict[str, dict[str, Any]] = {}
_erp_id_by_order: dict[str, str] = {}


class ErpAck(BaseModel):
    erp_order_id: str
    duplicate: bool


class ErpStats(BaseModel):
    total_orders: int
    failure_rate: float
    orders: list[dict[str, Any]]


@app.post("/erp/orders", response_model=ErpAck, status_code=status.HTTP_201_CREATED)
async def receive_order(
    payload: dict[str, Any],
    idempotency_key: str = Header(default="", alias="Idempotency-Key"),
) -> ErpAck:
    already_received = _acknowledged_by_key.get(idempotency_key) if idempotency_key else None
    if already_received is not None:
        return ErpAck(erp_order_id=already_received["erp_order_id"], duplicate=True)

    await asyncio.sleep(settings.erp_latency_ms / 1000)
    await _maybe_fail()

    record = _register(payload)
    if idempotency_key:
        _acknowledged_by_key[idempotency_key] = record
    return ErpAck(erp_order_id=record["erp_order_id"], duplicate=False)


@app.get("/erp/orders", response_model=ErpStats)
async def list_orders() -> ErpStats:
    return ErpStats(
        total_orders=len(_erp_id_by_order),
        failure_rate=settings.erp_failure_rate,
        orders=list(_acknowledged_by_key.values()),
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def _maybe_fail() -> None:
    if random.random() >= settings.erp_failure_rate:
        return

    if random.random() < TIMEOUT_SHARE_OF_FAILURES:
        await asyncio.sleep(settings.erp_timeout_seconds + 1)

    raise HTTPException(
        status_code=random.choice(SERVER_ERROR_CODES),
        detail="ERP indisponivel (falha injetada)",
    )


def _register(payload: dict[str, Any]) -> dict[str, Any]:
    order = payload.get("order", {})
    order_uid = str(order.get("order_uid", "desconhecido"))
    erp_order_id = f"ERP-{uuid.uuid4().hex[:10].upper()}"

    _erp_id_by_order[order_uid] = erp_order_id
    return {
        "erp_order_id": erp_order_id,
        "order_uid": order_uid,
        "received_at": datetime.now(UTC).isoformat(),
        "items": len(payload.get("items", [])),
        "total_amount": order.get("total_amount"),
        "city": payload.get("shipping_address", {}).get("city"),
    }
