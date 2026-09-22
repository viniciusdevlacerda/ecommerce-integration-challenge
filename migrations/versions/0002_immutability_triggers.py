"""Imutabilidade do pedido fechado.

Revision ID: 0002
Revises: 0001
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_ORDERS_TRIGGER = """
CREATE TRIGGER TR_orders_block_retroactive_change
ON orders
AFTER UPDATE, DELETE
AS
-- Bloqueia alteracao de colunas financeiras e de vinculo apos o status virar
-- terminal. Avancar o STATUS continua permitido: e o que separa "imutavel" de
-- "congelado". Ultima linha de defesa contra qualquer caminho que nao passe
-- pela aplicacao.
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
          FROM deleted d
          LEFT JOIN inserted i ON i.order_id = d.order_id
         WHERE d.status IN ('APPROVED', 'INVOICED', 'CANCELLED')
           AND (
                i.order_id IS NULL
             OR i.total_amount        <> d.total_amount
             OR i.client_id           <> d.client_id
             OR i.shipping_address_id <> d.shipping_address_id
             OR i.billing_address_id  <> d.billing_address_id
             OR i.order_uid           <> d.order_uid
             OR i.currency            <> d.currency
             OR i.placed_at           <> d.placed_at
           )
    )
    BEGIN
        THROW 50001,
              'Pedido fechado e imutavel: alteracao retroativa bloqueada.',
              1;
    END
END
"""

_ITEMS_TRIGGER = """
CREATE TRIGGER TR_order_items_block_retroactive_change
ON order_items
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
          FROM orders o
         WHERE o.status IN ('APPROVED', 'INVOICED', 'CANCELLED')
           AND o.order_id IN (
                SELECT order_id FROM inserted
                UNION
                SELECT order_id FROM deleted
           )
    )
    BEGIN
        THROW 50002,
              'Itens de pedido fechado sao imutaveis.',
              1;
    END
END
"""


def upgrade() -> None:
    op.execute(_ORDERS_TRIGGER)
    op.execute(_ITEMS_TRIGGER)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS TR_order_items_block_retroactive_change")
    op.execute("DROP TRIGGER IF EXISTS TR_orders_block_retroactive_change")
