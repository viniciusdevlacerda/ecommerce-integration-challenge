"""Tabelas da carga em massa: staging em heap e fato em columnstore.

Revision ID: 0003
Revises: 0002
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_STAGING = """
CREATE TABLE stg_order_history (
    order_uid        VARCHAR(64)     NOT NULL,
    client_uid       VARCHAR(64)     NOT NULL,
    client_document  VARCHAR(14)     NOT NULL,
    order_date       DATE            NOT NULL,
    channel          VARCHAR(20)     NOT NULL,
    payment_method   VARCHAR(16)     NOT NULL,
    status           VARCHAR(16)     NOT NULL,
    sku              VARCHAR(40)     NOT NULL,
    product_name     NVARCHAR(120)   NOT NULL,
    category         VARCHAR(40)     NOT NULL,
    quantity         INT             NOT NULL,
    unit_price       DECIMAL(19,4)   NOT NULL,
    freight_amount   DECIMAL(19,4)   NOT NULL,
    total_amount     DECIMAL(19,4)   NOT NULL,
    state            CHAR(2)         NOT NULL,
    city             NVARCHAR(80)    NOT NULL
)
"""

_FACT = """
CREATE TABLE fact_order_history (
    order_uid        VARCHAR(64)     NOT NULL,
    client_uid       VARCHAR(64)     NOT NULL,
    client_document  VARCHAR(14)     NOT NULL,
    order_date       DATE            NOT NULL,
    channel          VARCHAR(20)     NOT NULL,
    payment_method   VARCHAR(16)     NOT NULL,
    status           VARCHAR(16)     NOT NULL,
    sku              VARCHAR(40)     NOT NULL,
    product_name     NVARCHAR(120)   NOT NULL,
    category         VARCHAR(40)     NOT NULL,
    quantity         INT             NOT NULL,
    unit_price       DECIMAL(19,4)   NOT NULL,
    freight_amount   DECIMAL(19,4)   NOT NULL,
    total_amount     DECIMAL(19,4)   NOT NULL,
    state            CHAR(2)         NOT NULL,
    city             NVARCHAR(80)    NOT NULL,
    loaded_at        DATETIME2(0)    NOT NULL CONSTRAINT df_fact_loaded_at DEFAULT SYSUTCDATETIME()
)
"""

_COLUMNSTORE = """
CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_order_history
    ON fact_order_history
"""


def upgrade() -> None:
    op.execute(_STAGING)
    op.execute(_FACT)
    op.execute(_COLUMNSTORE)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS fact_order_history")
    op.execute("DROP TABLE IF EXISTS stg_order_history")
