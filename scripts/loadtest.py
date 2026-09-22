from __future__ import annotations

import argparse
import asyncio
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx

API = "http://localhost:8000"
PARTNER = "loadtest"


def build_batch(size: int) -> list[dict[str, Any]]:
    now = datetime.now(UTC).isoformat()
    return [
        {
            "event_id": f"lt-{uuid.uuid4().hex}",
            "event_type": "client.upserted",
            "occurred_at": now,
            "partner": PARTNER,
            "data": {
                "client_uid": f"LT-CLI-{uuid.uuid4().hex[:12]}",
                "full_name": "Cliente Teste de Carga",
                "document": "39053344705",
                "email": "carga@exemplo.com.br",
                "address": {
                    "address_uid": f"LT-ADDR-{uuid.uuid4().hex[:12]}",
                    "street": "Avenida das Industrias",
                    "number": "100",
                    "complement": None,
                    "district": "Centro",
                    "city": "Joinville",
                    "state": "SC",
                    "zip_code": "89219000",
                },
            },
        }
        for _ in range(size)
    ]


async def worker(
    client: httpx.AsyncClient, requests: int, batch_size: int, stats: dict[str, int]
) -> None:
    for _ in range(requests):
        try:
            response = await client.post(
                f"/webhooks/{PARTNER}/events", json=build_batch(batch_size), timeout=20
            )
        except httpx.HTTPError:
            stats["erro"] += 1
            continue

        if response.status_code == 202:
            stats["aceitos"] += response.json()["accepted"]
            stats["http_202"] += 1
        elif response.status_code == 429:
            stats["http_429"] += 1  # backpressure: comportamento correto
        else:
            stats["erro"] += 1


async def main() -> None:
    parser = argparse.ArgumentParser(description="Load test da API de webhooks")
    parser.add_argument("--total", type=int, default=20_000)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()

    requests_total = max(1, args.total // args.batch_size)
    per_worker = max(1, requests_total // args.concurrency)
    stats = {"aceitos": 0, "http_202": 0, "http_429": 0, "erro": 0}

    limits = httpx.Limits(max_connections=args.concurrency * 2)
    async with httpx.AsyncClient(base_url=API, limits=limits) as client:
        start = time.perf_counter()
        await asyncio.gather(
            *(worker(client, per_worker, args.batch_size, stats) for _ in range(args.concurrency))
        )
        elapsed = time.perf_counter() - start

    total = stats["aceitos"]
    print(f"\n{'=' * 60}")
    print(f" eventos aceitos ........ {total:,}")
    print(f" tempo .................. {elapsed:.2f}s")
    print(f" vazao .................. {total / elapsed:,.0f} eventos/s")
    print(f" HTTP 202 ............... {stats['http_202']:,}")
    print(f" HTTP 429 (backpressure)  {stats['http_429']:,}")
    print(f" erros .................. {stats['erro']:,}")
    print(f"{'=' * 60}")
    print(" 429 nao e falha: e a borda pedindo ao parceiro para diminuir o ritmo.\n")


if __name__ == "__main__":
    asyncio.run(main())
