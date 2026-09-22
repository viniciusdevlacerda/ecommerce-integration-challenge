from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.shared.domain.enums import EventType, PaymentMethod, PaymentStatus

Money = Annotated[Decimal, Field(max_digits=19, decimal_places=4, ge=0)]


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=8, max_length=64)
    event_type: EventType
    occurred_at: datetime
    partner: str = Field(min_length=1, max_length=40)
    data: dict[str, Any]


class AddressPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address_uid: str = Field(min_length=1, max_length=64)
    street: str = Field(max_length=160)
    number: str = Field(max_length=20)
    complement: str | None = Field(default=None, max_length=80)
    district: str = Field(max_length=80)
    city: str = Field(max_length=80)
    state: str = Field(min_length=2, max_length=2)
    zip_code: str = Field(min_length=8, max_length=8)

    @field_validator("state")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()


class ClientUpsertedData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_uid: str = Field(min_length=1, max_length=64)
    full_name: str = Field(max_length=160)
    document: str = Field(min_length=11, max_length=14)
    email: str = Field(max_length=160)
    address: AddressPayload


class OrderItemPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sku: str = Field(max_length=40)
    description: str = Field(max_length=200)
    quantity: int = Field(gt=0)
    unit_price: Money


class OrderCreatedData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_uid: str = Field(min_length=1, max_length=64)
    client_uid: str = Field(min_length=1, max_length=64)
    shipping_address_uid: str = Field(min_length=1, max_length=64)
    billing_address_uid: str | None = None
    currency: str = Field(default="BRL", min_length=3, max_length=3)
    placed_at: datetime
    items: list[OrderItemPayload] = Field(min_length=1)

    @property
    def total_amount(self) -> Decimal:
        return sum((i.unit_price * i.quantity for i in self.items), Decimal("0"))


class PaymentUpdatedData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_uid: str = Field(min_length=1, max_length=64)
    provider: str = Field(max_length=40)
    provider_tx_id: str = Field(max_length=80)
    method: PaymentMethod
    status: PaymentStatus
    amount: Money
    occurred_at: datetime


class InvoiceIssuedData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_uid: str = Field(min_length=1, max_length=64)
    invoice_number: str = Field(max_length=40)
    series: str = Field(max_length=10)
    access_key: str = Field(max_length=44)
    issued_at: datetime
    total_amount: Money
