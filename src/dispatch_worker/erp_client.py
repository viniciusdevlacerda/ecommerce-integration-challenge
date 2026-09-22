from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from typing import Any

import httpx

from src.shared.config import Settings

logger = logging.getLogger(__name__)

ORDERS_ENDPOINT = "/erp/orders"
RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
CLIENT_ERROR_THRESHOLD = 400


class ErpTemporaryError(RuntimeError):
    pass


class ErpPermanentError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ErpResponse:
    erp_order_id: str
    duplicate: bool


class ErpClient:
    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        self._client = client
        self._settings = settings

    async def send_order(self, *, idempotency_key: str, payload: dict[str, Any]) -> ErpResponse:
        response = await self._post(idempotency_key, payload)
        self._raise_for_status(response)

        body = response.json()
        return ErpResponse(
            erp_order_id=str(body.get("erp_order_id", "")),
            duplicate=bool(body.get("duplicate", False)),
        )

    async def _post(self, idempotency_key: str, payload: dict[str, Any]) -> httpx.Response:
        try:
            return await self._client.post(
                ORDERS_ENDPOINT,
                json=payload,
                headers={"Idempotency-Key": idempotency_key},
                timeout=self._settings.erp_timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            raise ErpTemporaryError(f"timeout ao chamar o ERP: {exc}") from exc
        except httpx.TransportError as exc:
            raise ErpTemporaryError(f"falha de transporte: {exc}") from exc

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.status_code in RETRYABLE_STATUS_CODES:
            raise ErpTemporaryError(f"ERP respondeu {response.status_code}")
        if response.status_code >= CLIENT_ERROR_THRESHOLD:
            raise ErpPermanentError(
                f"ERP recusou ({response.status_code}): {response.text[:200]}"
            )


def backoff_delay(attempt: int, settings: Settings) -> float:
    # Exponencial com jitter: sem a aleatoriedade, todas as mensagens pendentes
    # voltam no mesmo instante e derrubam o ERP de novo assim que ele levanta.
    base = settings.dispatch_backoff_base_seconds
    ceiling = min(settings.dispatch_backoff_max_seconds, base * 2**attempt)
    return random.uniform(base, max(ceiling, base))
