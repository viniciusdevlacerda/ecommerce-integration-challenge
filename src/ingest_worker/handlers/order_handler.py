from __future__ import annotations

from typing import ClassVar

from src.ingest_worker.handlers.base import DependencyNotReadyError
from src.shared.db.models import Address
from src.shared.db.uow import UnitOfWork
from src.shared.domain.enums import EventType
from src.shared.domain.events import EventEnvelope, OrderCreatedData


class OrderCreatedHandler:
    event_type: ClassVar[EventType] = EventType.ORDER_CREATED

    def handle(self, uow: UnitOfWork, envelope: EventEnvelope) -> None:
        data = OrderCreatedData.model_validate(envelope.data)

        if uow.orders.exists(data.order_uid):
            return

        client = uow.clients.get_by_uid(data.client_uid)
        if client is None:
            raise DependencyNotReadyError(f"cliente {data.client_uid} ainda nao ingerido")

        shipping = self._require_current_address(uow, data.shipping_address_uid)
        billing = self._resolve_billing_address(uow, data, shipping)

        uow.orders.create(
            order_uid=data.order_uid,
            client_id=client.client_id,
            shipping_address_id=shipping.address_id,
            billing_address_id=billing.address_id,
            currency=data.currency,
            total_amount=data.total_amount,
            placed_at=data.placed_at.replace(tzinfo=None),
            source_event_id=envelope.event_id,
            items=[item.model_dump() for item in data.items],
        )

    def _resolve_billing_address(
        self, uow: UnitOfWork, data: OrderCreatedData, shipping: Address
    ) -> Address:
        uses_shipping_address = (
            not data.billing_address_uid
            or data.billing_address_uid == data.shipping_address_uid
        )
        if uses_shipping_address:
            return shipping
        return self._require_current_address(uow, data.billing_address_uid)

    @staticmethod
    def _require_current_address(uow: UnitOfWork, address_uid: str | None) -> Address:
        address = uow.addresses.get_current_version(str(address_uid))
        if address is None:
            raise DependencyNotReadyError(f"endereco {address_uid} ausente")
        return address
