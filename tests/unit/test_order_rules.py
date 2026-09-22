"""Regras de dominio que protegem o pedido fechado."""

from __future__ import annotations

from src.shared.domain.enums import OrderStatus


def test_status_terminal_identifica_pedido_fechado() -> None:
    assert OrderStatus.APPROVED.is_terminal
    assert OrderStatus.INVOICED.is_terminal
    assert OrderStatus.CANCELLED.is_terminal
    assert not OrderStatus.PENDING.is_terminal


def test_total_do_pedido_vem_dos_itens_e_nao_do_parceiro() -> None:
    """Confiar no total enviado pelo webhook e como aceitar o preco do cliente."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from src.shared.domain.events import OrderCreatedData

    order = OrderCreatedData(
        order_uid="ORD-1",
        client_uid="CLI-1",
        shipping_address_uid="ADDR-1",
        placed_at=datetime.now(UTC),
        items=[
            {"sku": "SKU-AF-8000", "description": "Air Fryer 8L", "quantity": 2,
             "unit_price": "649.90"},
            {"sku": "SKU-LQ-1200", "description": "Liquidificador", "quantity": 1,
             "unit_price": "219.90"},
        ],  # type: ignore[arg-type]
    )

    assert order.total_amount == Decimal("1519.70")
