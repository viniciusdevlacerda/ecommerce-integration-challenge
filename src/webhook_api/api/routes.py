from __future__ import annotations

import logging

from fastapi import APIRouter, Response, status
from pydantic import BaseModel

from src.shared.domain.events import EventEnvelope
from src.webhook_api.api.deps import BufferDep, GuardDep
from src.webhook_api.backpressure import MIN_RETRY_AFTER_SECONDS
from src.webhook_api.buffer import BufferFullError

logger = logging.getLogger(__name__)

router = APIRouter()

HEALTHY = "ok"
DEGRADED = "degraded"


class AcceptedResponse(BaseModel):
    accepted: int
    buffered: int


class HealthResponse(BaseModel):
    status: str
    buffer_depth: int
    stream_lag: int


@router.post(
    "/webhooks/{partner}/events",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=AcceptedResponse,
    summary="Recebe eventos de parceiro (pedido, pagamento, faturamento)",
)
async def ingest_events(
    partner: str,
    events: list[EventEnvelope],
    response: Response,
    buffer: BufferDep,
    guard: GuardDep,
) -> AcceptedResponse:
    backpressure = await guard.evaluate()
    if backpressure.overloaded:
        logger.warning(
            "backpressure acionado", extra={"lag": backpressure.lag, "partner": partner}
        )
        _ask_partner_to_slow_down(response, backpressure.retry_after_seconds)
        return AcceptedResponse(accepted=0, buffered=buffer.depth)

    accepted = 0
    for event in events:
        try:
            buffer.submit(event)
        except BufferFullError:
            _ask_partner_to_slow_down(response, MIN_RETRY_AFTER_SECONDS)
            break
        accepted += 1

    # O caminho quente nao toca no SQL Server: o evento so e enfileirado. Se a
    # persistencia acontecesse aqui, a velocidade do banco seria o teto da API.
    return AcceptedResponse(accepted=accepted, buffered=buffer.depth)


@router.get("/health", response_model=HealthResponse, summary="Liveness e profundidade da fila")
async def health(buffer: BufferDep, guard: GuardDep) -> HealthResponse:
    backpressure = await guard.evaluate()
    return HealthResponse(
        status=DEGRADED if backpressure.overloaded else HEALTHY,
        buffer_depth=buffer.depth,
        stream_lag=backpressure.lag,
    )


def _ask_partner_to_slow_down(response: Response, retry_after_seconds: int) -> None:
    response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
    response.headers["Retry-After"] = str(retry_after_seconds)
