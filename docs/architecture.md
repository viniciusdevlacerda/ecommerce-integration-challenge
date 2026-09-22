# Arquitetura

## Caminho de um evento, do parceiro ao ERP

| # | Onde | O que acontece | Garantia obtida |
|---|---|---|---|
| 1 | `webhook-api` | valida o contrato (Pydantic) e responde **202 em ms** | banco nao e o teto da API; parceiro nao toma timeout |
| 2 | `EventBuffer` | acumula 500 eventos ou 5 ms e publica em pipeline | 1 round-trip por lote, nao por evento |
| 3 | `BackpressureGuard` | mede o lag a cada 500 ms; acima do limite devolve **429 + Retry-After** | o parceiro reduz o ritmo em vez de a memoria crescer |
| 4 | Redis Stream | buffer capado (`MAXLEN ~`), `noeviction` | teto de memoria; recusa e melhor que descarte silencioso |
| 5 | `ingest-worker` | `XREADGROUP` em lote; dedupe no Redis | duplicata imediata morre sem tocar no banco |
| 6 | `EventProcessor` | registra em `processed_events` **e** aplica o efeito na mesma transacao | atomicidade entre "processei" e "o efeito existe" |
| 7 | handler | regra de negocio via repositorios | constraints do banco barram o que escapou |
| 8 | `XACK` | **so depois do commit** | at-least-once + idempotencia = exactly-once |
| 9 | outbox | gravada na mesma transacao da aprovacao | elimina o dual write |
| 10 | `dispatch-worker` | `UPDATE ... OUTPUT` com `READPAST` | N workers concorrem sem se bloquear |
| 11 | `ErpClient` | POST com timeout, backoff + jitter, `Idempotency-Key` | resiliencia a falha temporaria; sem duplicata no destino |
| 12 | DLQ | esgotadas as tentativas, preserva o ultimo erro | nada se perde, nada trava a fila |

## Onde cada requisito do enunciado foi resolvido

| Requisito | Arquivo |
|---|---|
| Migrations versionadas | `migrations/versions/0001..0003` |
| Auditoria historica do cadastro | `address` SCD2 — `src/shared/db/repositories.py::AddressRepository.upsert_version` |
| Pedido fechado imutavel | `migrations/versions/0002_immutability_triggers.py` |
| Ingestao de alto throughput | `src/webhook_api/buffer.py` |
| Backpressure | `src/webhook_api/backpressure.py` |
| Idempotencia | `src/ingest_worker/processor.py` + constraints em `0001` |
| Controle de concorrencia no banco | `src/shared/db/engine.py` (pool) + RCSI em `scripts/bootstrap.py` |
| Carga de 10M sem OOM | `airflow/dags/ecommerce/generator.py` |
| Escrita mais veloz | `airflow/dags/ecommerce/bulk_loader.py` |
| Worker event-driven com retry | `src/dispatch_worker/` |

## Modelo de concorrencia

Os workers sao **assincronos no I/O** (Redis, HTTP) e **sincronos no banco**
(pyodbc nao tem driver async maduro). A escrita roda em `asyncio.to_thread`,
limitada pelo pool. Consequencia pratica: escalar consumidores aumenta vazao de
forma previsivel, e pico de trafego **nunca** vira pico de conexoes no SQL Server.

```
trafego ──▶ [borda 202] ──▶ [fila] ──▶ [pool fixo de 8 conexoes] ──▶ SQL Server
             elastico        absorve        constante conhecida
```

Cada elemento absorve variacao para que o proximo veja carga estavel. O banco,
que e o recurso mais caro e menos elastico, e o mais protegido.
