# Plano de Implementação — Desafio Sênior: Integração em Larga Escala

> **v2 — escopo enxuto.** Prazo: 3 dias. Premissa: **o avaliador vai subir o compose e testar.**
> Domínio: e-commerce de eletroportáteis → dados sintéticos realistas.
> Stack: FastAPI + Redis Streams · SQL Server 2022 · Airflow 2.10 · SQLAlchemy 2.0 + Alembic · Python 3.12.

---

## Princípio norteador desta versão

Como o avaliador vai **executar**, a prioridade nº 1 é **subir de primeira na máquina dele**.
Uma feature a mais que quebra o `docker compose up` custa mais nota do que ela soma.
Tudo que não é pedido pelo enunciado foi cortado. A justificativa dos cortes vai no README — cortar
conscientemente e explicar por quê também é sinal de senioridade.

### Cortado da v1 (e por quê)

| Cortado | Motivo |
|---|---|
| Temporal table no `clients` | SCD2 já resolve histórico. Dois mecanismos = confusão, não domínio. |
| Circuit breaker | Enunciado pede "política de retry". Backoff + jitter + DLQ cumprem com folga. |
| Table swap com `sp_rename` | `INSERT ... SELECT` resolve. |
| Prometheus, HMAC, pasta de ADR | Não pedidos. Decisões vão em seção do README. |
| Airflow em 3 containers | Vira 1 (standalone) + postgres. Menos superfície de falha na máquina do avaliador. |
| 2 réplicas de ingest-worker | 1 no padrão; README mostra `--scale ingest-worker=3` para provar o consumer group. |

### Mantido, porque é onde a nota está

SCD2 + FK de versão no pedido · idempotência em camadas · BULK INSERT via volume compartilhado ·
pool limitado + RCSI · outbox transacional · `mem_limit` no worker do Airflow.

---

## Dados sintéticos do domínio

Nada de `product_1`, `client_2`. O gerador usa catálogo de **eletroportáteis**: air fryer, liquidificador,
batedeira, ventilador, cafeteira, micro-ondas, purificador — com SKU, faixa de preço plausível em BRL,
peso e dimensões (importa para frete). Clientes com **CPF válido por dígito verificador**, CEP coerente
com a UF, pagamento em **PIX / cartão / boleto**.

O **`erp-mock` expõe um contrato no estilo de integração com ERP** (pedido + cliente + endereço + itens +
transportadora), que é exatamente o desenho de integração de ERP que você já fez em produção. Isso vira
argumento de entrevista: não é um POST genérico, é o formato que o ERP de um varejo realmente consome.

---

## Ambiente (prioridade máxima: subir de primeira)

**8 containers:** `mssql` · `redis` · `webhook-api` · `ingest-worker` · `dispatch-worker` · `erp-mock` ·
`airflow` (standalone, LocalExecutor) · `airflow-postgres`.

- **Healthcheck real** em mssql (`sqlcmd -Q "SELECT 1"`) e redis, com `depends_on: condition: service_healthy`.
  Sem isso, migration roda antes do banco aceitar conexão e o avaliador vê erro na primeira tentativa.
- `docker compose up` → migrations aplicam sozinhas → seed mínimo → tudo pronto.
- um script de demonstração → roda o fluxo ponta a ponta e imprime o resultado (evento → pedido → aprovação → POST no ERP).
- README avisa: **Docker com 8GB de RAM** e, no Apple Silicon, `platform: linux/amd64` no mssql (Rosetta).
- DAG com `row_count` default **1.000.000** (roda em minutos na máquina do avaliador);
  **10.000.000** via trigger config, com o número medido documentado.

---

## Parte 1 — Modelagem & Migrations (Alembic + SQLAlchemy 2.0 tipado)

**O requisito traduzido:** cadastro muda com o tempo, pedido fechado não muda nunca.

- **`clients`** — `client_id` PK, `client_uid` UNIQUE, nome, CPF, email.
- **`address`** — **SCD Tipo 2**: `address_id` PK (uma linha **por versão**), `address_uid` (chave lógica),
  `client_id` FK, logradouro/nº/compl./bairro/cidade/UF/CEP, `valid_from`, `valid_to`, `is_current`.
  Índice único filtrado em `address_uid WHERE is_current = 1`.
- **`orders`** — `order_id` PK, `order_uid` UNIQUE, `client_id` FK, `shipping_address_id` FK→**versão**,
  `billing_address_id` FK→versão, `status`, `total_amount` DECIMAL(19,4), `placed_at`, `closed_at`, `source_event_id`.
- **`order_items`** — exigido pelo payload da Parte 4 (SKU, descrição, qtd, preço unitário).
- **`payment`** — `order_id` FK, `provider`, `provider_tx_id`, `method` (PIX/CREDIT_CARD/BOLETO),
  `status`, `amount`, `approved_at`. **UNIQUE (`provider`, `provider_tx_id`)**.
- **`invoices`** — `order_id` FK **UNIQUE**, `invoice_number` UNIQUE, série, `issued_at`, `total_amount`.
- **`processed_events`** — `event_id` PK, tipo, hash do payload, timestamps, status.
- **`outbox`** — `outbox_id`, `aggregate_id`, `event_type`, `payload`, `status`, `attempts`,
  `next_attempt_at`, `locked_by`, `last_error`. Índice filtrado em (`status`, `next_attempt_at`).
- **`stg_order_history`** (heap) + **`fact_order_history`** (clustered columnstore) — alvo dos 10M.

**Por que SCD2 e não temporal table** (vai no README): não se cria FK apontando para history table.
Como o pedido precisa de integridade referencial contra a **versão exata** do endereço, essa versão
tem que ser linha de primeira classe com PK própria.

**Imutabilidade:** CHECK de máquina de estados + **trigger** que bloqueia UPDATE/DELETE em colunas
financeiras quando o status é terminal.

**Na migration inicial:** `READ_COMMITTED_SNAPSHOT ON` — leitores param de bloquear escritores.

---

## Parte 2 — Ingestão: Abordagem A (FastAPI + Redis Streams)

**Argumento do README:** webhook é HTTP e Kafka não recebe HTTP. Escolher "Kafka" não elimina a API —
só a esconde atrás de um REST Proxy menos controlável. A decisão real é *qual broker fica atrás da borda*.
Redis Streams entrega consumer group, ACK e stream capado com uma fração do footprint do Kafka.
Trocaria por Kafka em: retenção longa para replay, múltiplos consumer groups de times distintos, ou volume
acima do que um Redis single-node aguenta — e a interface do publisher é abstraída para que a troca
seja de uma classe.

**webhook-api**
- `POST /webhooks/{partner}/events` — Pydantic v2, exige `event_id`. Responde **202 em ms, sem tocar no SQL Server**.
- **Batching**: `asyncio.Queue` + flush por 500 itens ou 5ms → `XADD` em pipeline (1 RTT por lote).
- **Backpressure**: lag do consumer group acima do limite → **429 + `Retry-After`**. Stream capado (`MAXLEN ~`).
- Graceful shutdown drenando o buffer.

**ingest-worker**
- `XREADGROUP` COUNT=500 BLOCK=1000; **XACK só depois do commit** (at-least-once + idempotência = exactly-once na prática).
- `XAUTOCLAIM` recupera pendências de consumer morto; DLQ após N tentativas.

**Idempotência em 3 camadas**
1. Redis `SET NX EX` — duplicata imediata, descartada sem tocar no banco.
2. `processed_events.event_id` PK — duplicata tardia / worker que morreu antes do ACK.
3. `UNIQUE(order_id)` em invoices e `UNIQUE(provider, provider_tx_id)` em payment — **duplicata com `event_id` novo**.
   É a camada que a maioria esquece e a única que garante literalmente "não gera faturamento duplicado".

**Concorrência**
- Pool pyodbc **fixo** (8 conexões/worker) → nº de conexões é constante escolhida, não função do tráfego.
- `fast_executemany=True`; **lote ordenado por chave** → elimina deadlock por ordem inversa de lock.
- Retry com backoff em deadlock (erro 1205); transações curtas; sem `MERGE` em tabela quente.

---

## Parte 3 — DAG Airflow: carga em massa

**DAG `load_order_history_bulk`** — `Param row_count` (default 1M; 10M via trigger config).

1. `prepare_staging` — `stg_order_history` como **heap sem índice**, recovery BULK_LOGGED.
2. **Dynamic task mapping**, `max_active_tis_per_dag = 4`:
   - `generate_chunk` — **generator com `yield`** escrevendo CSV linha a linha. Memória O(1) (dezenas de MB
     para 1 mil ou 10 milhões). Catálogo de produtos pré-carregado + `random.Random(seed=chunk_id)` (reprodutível,
     muito mais rápido que Faker por linha).
   - `bulk_load_chunk` — `BULK INSERT ... WITH (TABLOCK, BATCHSIZE=100000, FORMAT='CSV')` lendo de
     **volume compartilhado entre worker e container do SQL Server**. O dado **não passa pelo driver Python**.
3. `load_fact` — `INSERT ... WITH (TABLOCK) SELECT` no `fact_order_history`.
4. `create_indexes` — **depois** da carga; clustered columnstore.
5. `validate_load` — `COUNT(*)` esperado, checagem de nulos, `UPDATE STATISTICS`. Falha a DAG se divergir.
6. `cleanup` — remove os CSVs.

**Anti-OOM provado:** `mem_limit: 2g` no container do Airflow. Se vazar memória, o container morre e a DAG falha.

**Benchmark no README** (tabela curta): `BULK INSERT` vs `pyodbc fast_executemany` vs linha a linha.
Medido com 100k linhas para os métodos lentos e extrapolado — **declarando a extrapolação**, porque
rodar 10M linha a linha levaria horas e esse é justamente o ponto.

---

## Parte 4 — Saída event-driven (Transactional Outbox)

**O problema:** marcar o pedido como APPROVED no banco e chamar o ERP por HTTP não são atômicos.
Commit ok + POST falhou = ERP nunca soube. POST ok + commit falhou = ERP conhece pedido inexistente.
**Solução:** não chamar HTTP nessa hora — gravar na `outbox` **na mesma transação** do pedido.

**dispatch-worker** (asyncio)
- Poll: `UPDATE TOP (50) outbox WITH (READPAST, UPDLOCK, ROWLOCK) SET status='IN_FLIGHT' ... OUTPUT inserted.*`
  → N workers na mesma fila **sem se bloquearem**.
- Monta o payload completo: cliente + **endereço na versão referenciada pelo pedido** + itens + pagamento.
- `POST` no **`erp-mock`** (FastAPI com injeção configurável de 500/503/timeout — resiliência demonstrável, não alegada).
- **Retry**: `httpx.AsyncClient` com timeout, backoff exponencial + **jitter** (`tenacity`),
  `Idempotency-Key` no header, `max_attempts` → status **DLQ** com `last_error` preservado.

---

## Estrutura

```
ecommerce-integration-challenge/
├── docker-compose.yml · .env.example · README.md
├── docs/ architecture.md · data-model.md (DER mermaid)
├── src/
│   ├── shared/          config · db · models · schemas · logging
│   ├── webhook_api/     FastAPI de ingestão
│   ├── ingest_worker/   consumer do Redis Stream
│   ├── dispatch_worker/ relay do outbox
│   └── erp_mock/        ERP fictício com injeção de falhas
├── migrations/          alembic
├── airflow/dags/        load_order_history.py
├── tests/               idempotência · concorrência · retry
└── scripts/             loadtest.py · send_duplicates.py · seed.py
```

Qualidade: `ruff` + `mypy` + `pytest`.

---

## Cronograma de 3 dias

| Dia | Entrega |
|---|---|
| **1** | Scaffold, Dockerfiles, `docker-compose` **subindo limpo** com healthchecks · Parte 1 (modelos + migrations + seed) |
| **2** | Parte 2 (webhook-api + ingest-worker + idempotência) · Parte 4 (outbox + dispatch-worker + erp-mock) |
| **3** | Parte 3 (DAG + benchmark) · testes dos 3 pontos críticos · demo ponta a ponta · README e docs |

**Regra de corte:** se o dia 3 apertar, sacrifico nessa ordem — benchmark extrapolado → load test →
`XAUTOCLAIM`. O núcleo (4 partes funcionando + README) não é negociável.

> Git inicializado, **sem commits** sem seu OK.
