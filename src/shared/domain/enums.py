from __future__ import annotations

from enum import StrEnum

TERMINAL_ORDER_STATUSES = frozenset({"APPROVED", "INVOICED", "CANCELLED"})


class EventType(StrEnum):
    CLIENT_UPSERTED = "client.upserted"
    ORDER_CREATED = "order.created"
    PAYMENT_UPDATED = "payment.updated"
    INVOICE_ISSUED = "invoice.issued"


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    INVOICED = "INVOICED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self.value in TERMINAL_ORDER_STATUSES


ALLOWED_ORDER_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset({OrderStatus.APPROVED, OrderStatus.CANCELLED}),
    OrderStatus.APPROVED: frozenset({OrderStatus.INVOICED, OrderStatus.CANCELLED}),
    OrderStatus.INVOICED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
}


class PaymentMethod(StrEnum):
    PIX = "PIX"
    CREDIT_CARD = "CREDIT_CARD"
    BOLETO = "BOLETO"


class PaymentStatus(StrEnum):
    PENDING = "PENDING"
    AUTHORIZED = "AUTHORIZED"
    APPROVED = "APPROVED"
    REFUSED = "REFUSED"
    REFUNDED = "REFUNDED"


class OutboxStatus(StrEnum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    SENT = "SENT"
    DLQ = "DLQ"


class ProcessedEventStatus(StrEnum):
    DONE = "DONE"
    FAILED = "FAILED"
