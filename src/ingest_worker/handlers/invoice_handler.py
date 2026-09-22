from __future__ import annotations

from typing import ClassVar

from src.ingest_worker.handlers.base import DependencyNotReadyError
from src.shared.db.uow import UnitOfWork
from src.shared.domain.enums import EventType, OrderStatus
from src.shared.domain.events import EventEnvelope, InvoiceIssuedData


class InvoiceIssuedHandler:
    event_type: ClassVar[EventType] = EventType.INVOICE_ISSUED

    def handle(self, uow: UnitOfWork, envelope: EventEnvelope) -> None:
        data = InvoiceIssuedData.model_validate(envelope.data)

        order = uow.orders.get_by_uid(data.order_uid)
        if order is None:
            raise DependencyNotReadyError(f"pedido {data.order_uid} ainda nao ingerido")

        if uow.invoices.exists_for_order(order.order_id):
            return

        issued_at = data.issued_at.replace(tzinfo=None)
        uow.invoices.create(
            order_id=order.order_id,
            invoice_number=data.invoice_number,
            series=data.series,
            access_key=data.access_key,
            issued_at=issued_at,
            total_amount=data.total_amount,
        )
        uow.orders.transition(order, OrderStatus.INVOICED, closed_at=issued_at)
