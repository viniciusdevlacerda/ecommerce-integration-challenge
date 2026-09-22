from __future__ import annotations

from typing import Any

from redis import Redis
from sqlalchemy import text

from src.shared.config import get_settings
from src.shared.db.engine import get_engine

settings = get_settings()
engine = get_engine(settings)

WIDTH = 64


def _scalar(sql: str) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar()


def _rows(sql: str) -> list[Any]:
    with engine.connect() as conn:
        return conn.execute(text(sql)).all()


def _title(text_: str) -> None:
    print(f"\n{text_}\n{'-' * WIDTH}")


def _line(label: str, value: Any, note: str = "") -> None:
    dots = "." * max(2, 34 - len(label))
    suffix = f"   {note}" if note else ""
    print(f"  {label} {dots} {value:>10}{suffix}")


def cadastro() -> None:
    _title("CADASTRO")
    _line("clientes", _scalar("SELECT COUNT(*) FROM clients"))
    _line("enderecos (todas as versoes)", _scalar("SELECT COUNT(*) FROM address"))
    _line("enderecos vigentes", _scalar("SELECT COUNT(*) FROM address WHERE is_current = 1"))
    versionados = _scalar(
        "SELECT COUNT(*) FROM (SELECT address_uid FROM address "
        "GROUP BY address_uid HAVING COUNT(*) > 1) t"
    )
    _line("enderecos ja versionados", versionados, "SCD Tipo 2 em acao")


def pedidos() -> None:
    _title("PEDIDOS")
    for status, total in _rows("SELECT status, COUNT(*) FROM orders GROUP BY status"):
        _line(f"status {status}", total)
    _line("total de pedidos", _scalar("SELECT COUNT(*) FROM orders"))
    _line("itens", _scalar("SELECT COUNT(*) FROM order_items"))
    _line("pagamentos", _scalar("SELECT COUNT(*) FROM payment"))
    _line("faturas", _scalar("SELECT COUNT(*) FROM invoices"))


def idempotencia() -> None:
    _title("IDEMPOTENCIA")
    _line("eventos processados (ledger)", _scalar("SELECT COUNT(*) FROM processed_events"))

    pedidos_dup = _scalar(
        "SELECT COUNT(*) FROM (SELECT order_uid FROM orders "
        "GROUP BY order_uid HAVING COUNT(*) > 1) t"
    )
    faturas_dup = _scalar(
        "SELECT COUNT(*) FROM (SELECT order_id FROM invoices "
        "GROUP BY order_id HAVING COUNT(*) > 1) t"
    )
    pagamentos_dup = _scalar(
        "SELECT COUNT(*) FROM (SELECT provider, provider_tx_id FROM payment "
        "GROUP BY provider, provider_tx_id HAVING COUNT(*) > 1) t"
    )
    _line("pedidos duplicados", pedidos_dup, "esperado: 0")
    _line("faturas duplicadas", faturas_dup, "esperado: 0")
    _line("pagamentos duplicados", pagamentos_dup, "esperado: 0")


def integracao() -> None:
    _title("INTEGRACAO COM O ERP (outbox)")
    rows = _rows(
        "SELECT status, COUNT(*), MAX(attempts) FROM outbox GROUP BY status"
    )
    if not rows:
        _line("outbox", "vazia")
    for status, total, tentativas in rows:
        _line(f"status {status}", total, f"maximo de {tentativas} tentativa(s)")


def carga() -> None:
    _title("CARGA EM MASSA")
    staging = _scalar("SELECT COUNT_BIG(*) FROM stg_order_history")
    fato = _scalar("SELECT COUNT_BIG(*) FROM fact_order_history")
    _line("stg_order_history", f"{staging:,}".replace(",", "."))
    _line("fact_order_history", f"{fato:,}".replace(",", "."), "columnstore")


def fila() -> None:
    _title("FILA")
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        pendentes = redis.xlen(settings.stream_name)
        dlq = redis.xlen(f"{settings.stream_name}:dlq")
        _line("stream de ingestao", pendentes)
        _line("dead letter queue", dlq, "esperado: 0")
    except Exception as exc:
        _line("redis", "indisponivel", str(exc)[:40])
    finally:
        redis.close()


def main() -> None:
    print("=" * WIDTH)
    print(" ESTADO DO SISTEMA".center(WIDTH))
    print("=" * WIDTH)
    cadastro()
    pedidos()
    idempotencia()
    integracao()
    carga()
    fila()
    print(f"\n{'=' * WIDTH}\n")


if __name__ == "__main__":
    main()
