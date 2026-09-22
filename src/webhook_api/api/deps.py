from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request

from src.shared.config import Settings, get_settings
from src.webhook_api.backpressure import BackpressureGuard
from src.webhook_api.buffer import EventBuffer


def get_buffer(request: Request) -> EventBuffer:
    return request.app.state.buffer  # type: ignore[no-any-return]


def get_guard(request: Request) -> BackpressureGuard:
    return request.app.state.guard  # type: ignore[no-any-return]


BufferDep = Annotated[EventBuffer, Depends(get_buffer)]
GuardDep = Annotated[BackpressureGuard, Depends(get_guard)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
