from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from src.shared.db.models import (
    Address,
    Client,
    Invoice,
    Order,
    OrderItem,
    OutboxMessage,
    Payment,
    ProcessedEvent,
)
from src.shared.domain.enums import ALLOWED_ORDER_TRANSITIONS, OrderStatus, OutboxStatus

ADDRESS_VERSIONED_FIELDS = (
    "street",
    "number",
    "complement",
    "district",
    "city",
    "state",
    "zip_code",
)

MAX_ERROR_LENGTH = 1000


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class _Repository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def _count(self, statement: object) -> int:
        return int(self.session.scalar(statement) or 0)  # type: ignore[arg-type]


class ClientRepository(_Repository):
    def get_by_uid(self, client_uid: str) -> Client | None:
        return self.session.scalar(select(Client).where(Client.client_uid == client_uid))

    def upsert(self, *, client_uid: str, full_name: str, document: str, email: str) -> Client:
        client = self.get_by_uid(client_uid)
        if client is None:
            return self._insert(
                client_uid=client_uid, full_name=full_name, document=document, email=email
            )

        client.full_name = full_name
        client.document = document
        client.email = email
        client.updated_at = utcnow()
        return client

    def _insert(self, *, client_uid: str, full_name: str, document: str, email: str) -> Client:
        client = Client(
            client_uid=client_uid, full_name=full_name, document=document, email=email
        )
        self.session.add(client)
        self.session.flush()
        return client


class AddressRepository(_Repository):
    def get_current_version(self, address_uid: str) -> Address | None:
        return self.session.scalar(
            select(Address).where(
                Address.address_uid == address_uid,
                Address.is_current.is_(True),
            )
        )

    def get_by_id(self, address_id: int) -> Address | None:
        return self.session.get(Address, address_id)

    def upsert_version(self, *, client_id: int, payload: dict[str, str | None]) -> Address:
        address_uid = str(payload["address_uid"])
        current = self.get_current_version(address_uid)

        if current is not None and not self._differs_from(current, payload):
            return current

        changed_at = utcnow()
        if current is not None:
            self._close_version(current, closed_at=changed_at)

        return self._open_version(
            client_id=client_id, payload=payload, opened_at=changed_at
        )

    def _close_version(self, address: Address, *, closed_at: datetime) -> None:
        address.valid_to = closed_at
        address.is_current = False
        self.session.flush()

    def _open_version(
        self, *, client_id: int, payload: dict[str, str | None], opened_at: datetime
    ) -> Address:
        address = Address(
            address_uid=str(payload["address_uid"]),
            client_id=client_id,
            street=payload["street"],
            number=payload["number"],
            complement=payload.get("complement"),
            district=payload["district"],
            city=payload["city"],
            state=payload["state"],
            zip_code=payload["zip_code"],
            valid_from=opened_at,
            valid_to=None,
            is_current=True,
        )
        self.session.add(address)
        self.session.flush()
        return address

    @staticmethod
    def _differs_from(address: Address, payload: dict[str, str | None]) -> bool:
        return any(
            getattr(address, field) != payload.get(field)
            for field in ADDRESS_VERSIONED_FIELDS
        )


class OrderRepository(_Repository):
    def get_by_uid(self, order_uid: str) -> Order | None:
        return self.session.scalar(select(Order).where(Order.order_uid == order_uid))

    def exists(self, order_uid: str) -> bool:
        return self._count(
            select(func.count()).select_from(Order).where(Order.order_uid == order_uid)
        ) > 0

    def create(
        self,
        *,
        order_uid: str,
        client_id: int,
        shipping_address_id: int,
        billing_address_id: int,
        currency: str,
        total_amount: Decimal,
        placed_at: datetime,
        source_event_id: str,
        items: list[dict[str, object]],
    ) -> Order:
        order = Order(
            order_uid=order_uid,
            client_id=client_id,
            shipping_address_id=shipping_address_id,
            billing_address_id=billing_address_id,
            status=OrderStatus.PENDING.value,
            currency=currency,
            total_amount=total_amount,
            placed_at=placed_at,
            source_event_id=source_event_id,
        )
        self.session.add(order)
        self.session.flush()

        self._add_items(order, items)
        return order

    def _add_items(self, order: Order, items: list[dict[str, object]]) -> None:
        for item in items:
            quantity = int(item["quantity"])  # type: ignore[arg-type]
            unit_price = Decimal(str(item["unit_price"]))
            self.session.add(
                OrderItem(
                    order_id=order.order_id,
                    sku=str(item["sku"]),
                    description=str(item["description"]),
                    quantity=quantity,
                    unit_price=unit_price,
                    line_total=unit_price * quantity,
                )
            )
        self.session.flush()

    def transition(self, order: Order, new_status: OrderStatus, *, closed_at: datetime) -> bool:
        if not self._transition_is_allowed(order.status_enum, new_status):
            return False

        order.status = new_status.value
        order.closed_at = closed_at
        self.session.flush()
        return True

    @staticmethod
    def _transition_is_allowed(current: OrderStatus, target: OrderStatus) -> bool:
        return target in ALLOWED_ORDER_TRANSITIONS[current]


class PaymentRepository(_Repository):
    def get_by_provider_transaction(self, provider: str, provider_tx_id: str) -> Payment | None:
        return self.session.scalar(
            select(Payment).where(
                Payment.provider == provider,
                Payment.provider_tx_id == provider_tx_id,
            )
        )

    def upsert(
        self,
        *,
        order_id: int,
        provider: str,
        provider_tx_id: str,
        method: str,
        status: str,
        amount: Decimal,
        occurred_at: datetime,
        approved: bool,
    ) -> tuple[Payment, bool]:
        payment = self.get_by_provider_transaction(provider, provider_tx_id)
        was_created = payment is None

        if payment is None:
            payment = Payment(
                order_id=order_id,
                provider=provider,
                provider_tx_id=provider_tx_id,
                method=method,
                status=status,
                amount=amount,
            )
            self.session.add(payment)

        payment.status = status
        if approved and payment.approved_at is None:
            payment.approved_at = occurred_at

        self.session.flush()
        return payment, was_created

    def latest_for_order(self, order_id: int) -> Payment | None:
        return self.session.scalar(
            select(Payment)
            .where(Payment.order_id == order_id)
            .order_by(Payment.payment_id.desc())
            .limit(1)
        )


class InvoiceRepository(_Repository):
    def exists_for_order(self, order_id: int) -> bool:
        return self._count(
            select(func.count()).select_from(Invoice).where(Invoice.order_id == order_id)
        ) > 0

    def create(
        self,
        *,
        order_id: int,
        invoice_number: str,
        series: str,
        access_key: str,
        issued_at: datetime,
        total_amount: Decimal,
    ) -> Invoice:
        invoice = Invoice(
            order_id=order_id,
            invoice_number=invoice_number,
            series=series,
            access_key=access_key,
            issued_at=issued_at,
            total_amount=total_amount,
        )
        self.session.add(invoice)
        self.session.flush()
        return invoice


class ProcessedEventRepository(_Repository):
    def register(self, *, event_id: str, event_type: str, payload_hash: str) -> None:
        self.session.add(
            ProcessedEvent(event_id=event_id, event_type=event_type, payload_hash=payload_hash)
        )
        self.session.flush()

    def was_processed(self, event_id: str) -> bool:
        return self.session.get(ProcessedEvent, event_id) is not None


class OutboxRepository(_Repository):
    def enqueue(
        self,
        *,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: dict[str, object],
    ) -> OutboxMessage:
        message = OutboxMessage(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=json.dumps(payload, default=str, ensure_ascii=False),
            status=OutboxStatus.PENDING.value,
            next_attempt_at=utcnow(),
        )
        self.session.add(message)
        self.session.flush()
        return message

    def mark_sent(self, outbox_id: int) -> None:
        self._update(
            outbox_id,
            status=OutboxStatus.SENT.value,
            sent_at=utcnow(),
            locked_by=None,
            locked_at=None,
            last_error=None,
        )

    def mark_retry(self, outbox_id: int, *, next_attempt_at: datetime, error: str) -> None:
        self._update(
            outbox_id,
            status=OutboxStatus.PENDING.value,
            next_attempt_at=next_attempt_at,
            last_error=error[:MAX_ERROR_LENGTH],
            locked_by=None,
            locked_at=None,
        )

    def mark_dead_letter(self, outbox_id: int, *, error: str) -> None:
        self._update(
            outbox_id,
            status=OutboxStatus.DLQ.value,
            last_error=error[:MAX_ERROR_LENGTH],
            locked_by=None,
            locked_at=None,
        )

    def _update(self, outbox_id: int, **values: object) -> None:
        self.session.execute(
            update(OutboxMessage)
            .where(OutboxMessage.outbox_id == outbox_id)
            .values(**values)
        )
