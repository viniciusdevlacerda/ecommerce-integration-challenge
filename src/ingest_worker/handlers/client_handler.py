from __future__ import annotations

from typing import ClassVar

from src.shared.db.uow import UnitOfWork
from src.shared.domain.enums import EventType
from src.shared.domain.events import ClientUpsertedData, EventEnvelope


class ClientUpsertedHandler:
    event_type: ClassVar[EventType] = EventType.CLIENT_UPSERTED

    def handle(self, uow: UnitOfWork, envelope: EventEnvelope) -> None:
        data = ClientUpsertedData.model_validate(envelope.data)

        client = uow.clients.upsert(
            client_uid=data.client_uid,
            full_name=data.full_name,
            document=data.document,
            email=data.email,
        )
        uow.addresses.upsert_version(
            client_id=client.client_id, payload=data.address.model_dump()
        )
