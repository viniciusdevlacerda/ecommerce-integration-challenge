from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Numeric,
    String,
    Unicode,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.shared.db.base import Base
from src.shared.domain.enums import (
    OrderStatus,
    OutboxStatus,
    PaymentMethod,
    PaymentStatus,
    ProcessedEventStatus,
)

def _pk() -> Identity:
    return Identity(start=1, increment=1)


def _ts() -> Any:
    return mapped_column(DateTime(timezone=False), server_default=func.sysutcdatetime())


class Client(Base):
    __tablename__ = "clients"

    client_id: Mapped[int] = mapped_column(BigInteger, _pk(), primary_key=True)
    client_uid: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    full_name: Mapped[str] = mapped_column(Unicode(160))
    document: Mapped[str] = mapped_column(String(14))
    email: Mapped[str] = mapped_column(String(160))
    created_at: Mapped[datetime] = _ts()
    updated_at: Mapped[datetime] = _ts()

    addresses: Mapped[list[Address]] = relationship(back_populates="client")


class Address(Base):
    """SCD Tipo 2: uma linha por VERSAO do endereco.

    O pedido precisa de FK para o endereco exato vigente no fechamento, e nao se
    cria FK apontando para tabela de historico. Por isso cada versao e uma linha
    de primeira classe (`address_id`), e `address_uid` identifica o endereco
    logico ao longo do tempo.
    """

    __tablename__ = "address"
    __table_args__ = (
        Index(
            "ux_address_uid_current",
            "address_uid",
            unique=True,
            mssql_where=text("is_current = 1"),
        ),
        Index("ix_address_client", "client_id"),
    )

    address_id: Mapped[int] = mapped_column(BigInteger, _pk(), primary_key=True)
    address_uid: Mapped[str] = mapped_column(String(64))
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.client_id"))

    street: Mapped[str] = mapped_column(Unicode(160))
    number: Mapped[str] = mapped_column(String(20))
    complement: Mapped[str | None] = mapped_column(Unicode(80), nullable=True)
    district: Mapped[str] = mapped_column(Unicode(80))
    city: Mapped[str] = mapped_column(Unicode(80))
    state: Mapped[str] = mapped_column(String(2))
    zip_code: Mapped[str] = mapped_column(String(8))

    valid_from: Mapped[datetime] = _ts()
    valid_to: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, server_default=text("1"))

    client: Mapped[Client] = relationship(back_populates="addresses")


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','APPROVED','INVOICED','CANCELLED')",
            name="ck_orders_status",
        ),
        CheckConstraint("total_amount >= 0", name="ck_orders_total_positive"),
        Index("ix_orders_status", "status"),
    )

    order_id: Mapped[int] = mapped_column(BigInteger, _pk(), primary_key=True)
    order_uid: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.client_id"))

    # Versao do endereco vigente no fechamento -- nao o endereco "atual" do
    # cliente. E o que impede uma mudanca cadastral futura de reescrever o pedido.
    shipping_address_id: Mapped[int] = mapped_column(ForeignKey("address.address_id"))
    billing_address_id: Mapped[int] = mapped_column(ForeignKey("address.address_id"))

    status: Mapped[str] = mapped_column(String(16), server_default=text("'PENDING'"))
    currency: Mapped[str] = mapped_column(String(3), server_default=text("'BRL'"))
    total_amount: Mapped[Decimal] = mapped_column(Numeric(19, 4))
    placed_at: Mapped[datetime] = mapped_column(DateTime)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    source_event_id: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = _ts()

    items: Mapped[list[OrderItem]] = relationship(
        back_populates="order", cascade="all, delete-orphan"
    )
    client: Mapped[Client] = relationship()
    shipping_address: Mapped[Address] = relationship(foreign_keys=[shipping_address_id])
    billing_address: Mapped[Address] = relationship(foreign_keys=[billing_address_id])

    @property
    def status_enum(self) -> OrderStatus:
        return OrderStatus(self.status)

    @property
    def is_closed(self) -> bool:
        return self.status_enum.is_terminal


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        Index("ix_order_items_order", "order_id"),
        # O SQL Server recusa OUTPUT sem INTO quando a tabela tem trigger
        # habilitado para a operacao, e esta tem um AFTER INSERT que protege os
        # itens de pedido fechado. Sem desligar o implicit_returning, o
        # SQLAlchemy usaria OUTPUT para recuperar o IDENTITY e todo INSERT
        # falharia; desligado, ele recorre a SCOPE_IDENTITY().
        {"implicit_returning": False},
    )

    order_item_id: Mapped[int] = mapped_column(BigInteger, _pk(), primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.order_id"))
    sku: Mapped[str] = mapped_column(String(40))
    description: Mapped[str] = mapped_column(Unicode(200))
    quantity: Mapped[int] = mapped_column(Integer)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(19, 4))
    line_total: Mapped[Decimal] = mapped_column(Numeric(19, 4))

    order: Mapped[Order] = relationship(back_populates="items")


class Payment(Base):
    __tablename__ = "payment"
    __table_args__ = (
        # Idempotencia de negocio: a mesma transacao do provedor nunca vira dois
        # pagamentos, mesmo que chegue com event_id diferente.
        UniqueConstraint("provider", "provider_tx_id", name="ux_payment_provider_tx"),
        CheckConstraint(
            "method IN ('PIX','CREDIT_CARD','BOLETO')", name="ck_payment_method"
        ),
        Index("ix_payment_order", "order_id"),
    )

    payment_id: Mapped[int] = mapped_column(BigInteger, _pk(), primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.order_id"))
    provider: Mapped[str] = mapped_column(String(40))
    provider_tx_id: Mapped[str] = mapped_column(String(80))
    method: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16))
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4))
    authorized_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = _ts()

    order: Mapped[Order] = relationship()

    @property
    def method_enum(self) -> PaymentMethod:
        return PaymentMethod(self.method)

    @property
    def status_enum(self) -> PaymentStatus:
        return PaymentStatus(self.status)


class Invoice(Base):
    __tablename__ = "invoices"
    __table_args__ = (
        # "Evento duplicado nao gera faturamento duplicado" vira garantia do banco:
        # mesmo com a aplicacao inteira falhando, a segunda fatura e recusada.
        UniqueConstraint("order_id", name="ux_invoice_order"),
        UniqueConstraint("invoice_number", "series", name="ux_invoice_number_series"),
    )

    invoice_id: Mapped[int] = mapped_column(BigInteger, _pk(), primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.order_id"))
    invoice_number: Mapped[str] = mapped_column(String(40))
    series: Mapped[str] = mapped_column(String(10))
    access_key: Mapped[str] = mapped_column(String(44))
    issued_at: Mapped[datetime] = mapped_column(DateTime)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(19, 4))
    status: Mapped[str] = mapped_column(String(16), server_default=text("'ISSUED'"))
    created_at: Mapped[datetime] = _ts()

    order: Mapped[Order] = relationship()


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(40))
    payload_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(
        String(16), server_default=text(f"'{ProcessedEventStatus.DONE.value}'")
    )
    attempt_count: Mapped[int] = mapped_column(Integer, server_default=text("1"))
    processed_at: Mapped[datetime] = _ts()


class OutboxMessage(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_dispatchable",
            "status",
            "next_attempt_at",
            mssql_where=text("status IN ('PENDING','IN_FLIGHT')"),
        ),
    )

    outbox_id: Mapped[int] = mapped_column(BigInteger, _pk(), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(40))
    aggregate_id: Mapped[str] = mapped_column(String(64))
    event_type: Mapped[str] = mapped_column(String(40))
    payload: Mapped[str] = mapped_column(Unicode(None))  # NVARCHAR(MAX)
    status: Mapped[str] = mapped_column(
        String(16), server_default=text(f"'{OutboxStatus.PENDING.value}'")
    )
    attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = _ts()
    locked_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Unicode(1000), nullable=True)
    created_at: Mapped[datetime] = _ts()
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


__all__ = [
    "Address",
    "Base",
    "Client",
    "Invoice",
    "Order",
    "OrderItem",
    "OutboxMessage",
    "Payment",
    "ProcessedEvent",
]
