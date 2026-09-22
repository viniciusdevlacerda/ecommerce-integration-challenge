from __future__ import annotations

from typing import Any

from src.shared.db.models import Address, Order, Payment


def build_order_payload(order: Order, payment: Payment | None) -> dict[str, Any]:
    return {
        "order": _order_section(order),
        "customer": _customer_section(order),
        "shipping_address": _address_section(order.shipping_address),
        "billing_address": _address_section(order.billing_address),
        "items": [_item_section(item) for item in order.items],
        "payment": _payment_section(payment),
    }


def _order_section(order: Order) -> dict[str, Any]:
    return {
        "order_uid": order.order_uid,
        "status": order.status,
        "currency": order.currency,
        "total_amount": str(order.total_amount),
        "placed_at": order.placed_at.isoformat(),
        "closed_at": order.closed_at.isoformat() if order.closed_at else None,
    }


def _customer_section(order: Order) -> dict[str, Any]:
    return {
        "client_uid": order.client.client_uid,
        "full_name": order.client.full_name,
        "document": order.client.document,
        "email": order.client.email,
    }


def _address_section(address: Address) -> dict[str, Any]:
    return {
        "address_uid": address.address_uid,
        "version_id": address.address_id,
        "street": address.street,
        "number": address.number,
        "complement": address.complement,
        "district": address.district,
        "city": address.city,
        "state": address.state,
        "zip_code": address.zip_code,
    }


def _item_section(item: Any) -> dict[str, Any]:
    return {
        "sku": item.sku,
        "description": item.description,
        "quantity": item.quantity,
        "unit_price": str(item.unit_price),
        "line_total": str(item.line_total),
    }


def _payment_section(payment: Payment | None) -> dict[str, Any] | None:
    if payment is None:
        return None
    return {
        "provider": payment.provider,
        "provider_tx_id": payment.provider_tx_id,
        "method": payment.method,
        "status": payment.status,
        "amount": str(payment.amount),
        "approved_at": payment.approved_at.isoformat() if payment.approved_at else None,
    }
