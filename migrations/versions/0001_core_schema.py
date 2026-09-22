"""Schema principal do e-commerce.

Revision ID: 0001
Revises:
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_NOW = sa.text("SYSUTCDATETIME()")


def _identity() -> sa.Identity:
    return sa.Identity(start=1, increment=1)


def upgrade() -> None:
    op.create_table(
        "clients",
        sa.Column("client_id", sa.BigInteger(), _identity(), nullable=False),
        sa.Column("client_uid", sa.String(64), nullable=False),
        sa.Column("full_name", sa.Unicode(160), nullable=False),
        sa.Column("document", sa.String(14), nullable=False),
        sa.Column("email", sa.String(160), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.PrimaryKeyConstraint("client_id"),
        sa.UniqueConstraint("client_uid", name="uq_clients_client_uid"),
    )
    op.create_index("ix_clients_client_uid", "clients", ["client_uid"])

    op.create_table(
        "address",
        sa.Column("address_id", sa.BigInteger(), _identity(), nullable=False),
        sa.Column("address_uid", sa.String(64), nullable=False),
        sa.Column("client_id", sa.BigInteger(), nullable=False),
        sa.Column("street", sa.Unicode(160), nullable=False),
        sa.Column("number", sa.String(20), nullable=False),
        sa.Column("complement", sa.Unicode(80), nullable=True),
        sa.Column("district", sa.Unicode(80), nullable=False),
        sa.Column("city", sa.Unicode(80), nullable=False),
        sa.Column("state", sa.String(2), nullable=False),
        sa.Column("zip_code", sa.String(8), nullable=False),
        sa.Column("valid_from", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.Column("valid_to", sa.DateTime(), nullable=True),
        sa.Column("is_current", sa.Boolean(), server_default=sa.text("1"), nullable=False),
        sa.PrimaryKeyConstraint("address_id"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.client_id"], name="fk_address_client"),
    )
    op.create_index("ix_address_client", "address", ["client_id"])
    op.create_index(
        "ux_address_uid_current",
        "address",
        ["address_uid"],
        unique=True,
        mssql_where=sa.text("is_current = 1"),
    )

    op.create_table(
        "orders",
        sa.Column("order_id", sa.BigInteger(), _identity(), nullable=False),
        sa.Column("order_uid", sa.String(64), nullable=False),
        sa.Column("client_id", sa.BigInteger(), nullable=False),
        sa.Column("shipping_address_id", sa.BigInteger(), nullable=False),
        sa.Column("billing_address_id", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(16), server_default=sa.text("'PENDING'"), nullable=False),
        sa.Column("currency", sa.String(3), server_default=sa.text("'BRL'"), nullable=False),
        sa.Column("total_amount", sa.Numeric(19, 4), nullable=False),
        sa.Column("placed_at", sa.DateTime(), nullable=False),
        sa.Column("closed_at", sa.DateTime(), nullable=True),
        sa.Column("source_event_id", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.PrimaryKeyConstraint("order_id"),
        sa.UniqueConstraint("order_uid", name="uq_orders_order_uid"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.client_id"], name="fk_orders_client"),
        sa.ForeignKeyConstraint(
            ["shipping_address_id"], ["address.address_id"], name="fk_orders_shipping_address"
        ),
        sa.ForeignKeyConstraint(
            ["billing_address_id"], ["address.address_id"], name="fk_orders_billing_address"
        ),
        sa.CheckConstraint(
            "status IN ('PENDING','APPROVED','INVOICED','CANCELLED')", name="ck_orders_status"
        ),
        sa.CheckConstraint("total_amount >= 0", name="ck_orders_total_positive"),
    )
    op.create_index("ix_orders_order_uid", "orders", ["order_uid"])
    op.create_index("ix_orders_status", "orders", ["status"])

    op.create_table(
        "order_items",
        sa.Column("order_item_id", sa.BigInteger(), _identity(), nullable=False),
        sa.Column("order_id", sa.BigInteger(), nullable=False),
        sa.Column("sku", sa.String(40), nullable=False),
        sa.Column("description", sa.Unicode(200), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("unit_price", sa.Numeric(19, 4), nullable=False),
        sa.Column("line_total", sa.Numeric(19, 4), nullable=False),
        sa.PrimaryKeyConstraint("order_item_id"),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"], name="fk_order_items_order"),
    )
    op.create_index("ix_order_items_order", "order_items", ["order_id"])

    op.create_table(
        "payment",
        sa.Column("payment_id", sa.BigInteger(), _identity(), nullable=False),
        sa.Column("order_id", sa.BigInteger(), nullable=False),
        sa.Column("provider", sa.String(40), nullable=False),
        sa.Column("provider_tx_id", sa.String(80), nullable=False),
        sa.Column("method", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("amount", sa.Numeric(19, 4), nullable=False),
        sa.Column("authorized_at", sa.DateTime(), nullable=True),
        sa.Column("approved_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.PrimaryKeyConstraint("payment_id"),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"], name="fk_payment_order"),
        sa.UniqueConstraint("provider", "provider_tx_id", name="ux_payment_provider_tx"),
        sa.CheckConstraint("method IN ('PIX','CREDIT_CARD','BOLETO')", name="ck_payment_method"),
    )
    op.create_index("ix_payment_order", "payment", ["order_id"])

    op.create_table(
        "invoices",
        sa.Column("invoice_id", sa.BigInteger(), _identity(), nullable=False),
        sa.Column("order_id", sa.BigInteger(), nullable=False),
        sa.Column("invoice_number", sa.String(40), nullable=False),
        sa.Column("series", sa.String(10), nullable=False),
        sa.Column("access_key", sa.String(44), nullable=False),
        sa.Column("issued_at", sa.DateTime(), nullable=False),
        sa.Column("total_amount", sa.Numeric(19, 4), nullable=False),
        sa.Column("status", sa.String(16), server_default=sa.text("'ISSUED'"), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.PrimaryKeyConstraint("invoice_id"),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"], name="fk_invoices_order"),
        sa.UniqueConstraint("order_id", name="ux_invoice_order"),
        sa.UniqueConstraint("invoice_number", "series", name="ux_invoice_number_series"),
    )

    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), server_default=sa.text("'DONE'"), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("processed_at", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.PrimaryKeyConstraint("event_id"),
    )

    op.create_table(
        "outbox",
        sa.Column("outbox_id", sa.BigInteger(), _identity(), nullable=False),
        sa.Column("aggregate_type", sa.String(40), nullable=False),
        sa.Column("aggregate_id", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column("payload", sa.Unicode(), nullable=False),  # NVARCHAR(MAX)
        sa.Column("status", sa.String(16), server_default=sa.text("'PENDING'"), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.Column("locked_by", sa.String(64), nullable=True),
        sa.Column("locked_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.Unicode(1000), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=_NOW, nullable=False),
        sa.Column("sent_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("outbox_id"),
    )
    op.create_index(
        "ix_outbox_dispatchable",
        "outbox",
        ["status", "next_attempt_at"],
        mssql_where=sa.text("status IN ('PENDING','IN_FLIGHT')"),
    )


def downgrade() -> None:
    for table in (
        "outbox",
        "processed_events",
        "invoices",
        "payment",
        "order_items",
        "orders",
        "address",
        "clients",
    ):
        op.drop_table(table)
