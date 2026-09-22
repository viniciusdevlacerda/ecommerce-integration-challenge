from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from src.shared.config import get_settings
from src.shared.logging import configure_logging
from src.shared.messaging.publisher import RedisStreamPublisher
from src.webhook_api.api.routes import router
from src.webhook_api.backpressure import BackpressureGuard
from src.webhook_api.buffer import EventBuffer

logger = logging.getLogger(__name__)

API_DESCRIPTION = (
    "Borda de ingestao de webhooks de parceiros. Responde 202 sem tocar no banco; "
    "o buffer publica em lote no Redis Stream e o backpressure devolve 429 quando "
    "os consumidores ficam para tras."
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)

    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    publisher = RedisStreamPublisher(redis, settings)
    buffer = EventBuffer(publisher, settings)
    await buffer.start()

    app.state.redis = redis
    app.state.buffer = buffer
    app.state.guard = BackpressureGuard(publisher, settings)
    logger.info("webhook-api pronta")

    try:
        yield
    finally:
        await buffer.stop()
        await redis.aclose()
        logger.info("webhook-api encerrada")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Webhook Ingestion API",
        version="1.0.0",
        description=API_DESCRIPTION,
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()
