from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from src.ingest_worker.handlers.base import DependencyNotReadyError
from src.shared.db.models import Order
from src.shared.db.uow import UnitOfWork
from src.shared.domain.enums import EventType, OrderStatus, PaymentStatus
from src.shared.domain.events import EventEnvelope, PaymentUpdatedData
from src.shared.domain.order_payload import build_order_payload

ORDER_APPROVED_EVENT = "order.approved"
ORDER_AGGREGATE = "order"


class PaymentUpdatedHandler:
    event_type: ClassVar[EventType] = EventType.PAYMENT_UPDATED

    def handle(self, uow: UnitOfWork, envelope: EventEnvelope) -> None:
        data = PaymentUpdatedData.model_validate(envelope.data)
        order = self._require_order(uow, data.order_uid)
        occurred_at = data.occurred_at.replace(tzinfo=None)

        self._record_payment(uow, order, data, occurred_at)

        if data.status is not PaymentStatus.APPROVED:
            return

        # Aprovar o pedido e enfileirar o aviso ao ERP acontecem na MESMA
        # transacao (outbox). Chamar HTTP aqui abriria o dual write: commit sem
        # POST deixa o ERP sem saber, POST sem commit avisa de um pedido que nao
        # existe. transition() devolve False se o pedido ja foi aprovado antes,
        # e por isso um evento duplicado nao gera um segundo despacho.
        approved_now = uow.orders.transition(
            order, OrderStatus.APPROVED, closed_at=occurred_at
        )
        if approved_now:
            self._enqueue_erp_notification(uow, order)

    @staticmethod
    def _require_order(uow: UnitOfWork, order_uid: str) -> Order:
        order = uow.orders.get_by_uid(order_uid)
        if order is None:
            raise DependencyNotReadyError(f"pedido {order_uid} ainda nao ingerido")
        return order

    @staticmethod
    def _record_payment(
        uow: UnitOfWork, order: Order, data: PaymentUpdatedData, occurred_at: datetime
    ) -> None:
        uow.payments.upsert(
            order_id=order.order_id,
            provider=data.provider,
            provider_tx_id=data.provider_tx_id,
            method=data.method.value,
            status=data.status.value,
            amount=data.amount,
            occurred_at=occurred_at,
            approved=data.status is PaymentStatus.APPROVED,
        )

    @staticmethod
    def _enqueue_erp_notification(uow: UnitOfWork, order: Order) -> None:
        payment = uow.payments.latest_for_order(order.order_id)
        uow.outbox.enqueue(
            aggregate_type=ORDER_AGGREGATE,
            aggregate_id=order.order_uid,
            event_type=ORDER_APPROVED_EVENT,
            payload=build_order_payload(order, payment),
        )
