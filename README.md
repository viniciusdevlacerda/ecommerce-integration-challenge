# Pipeline de Integração em Larga Escala — E-commerce

Protótipo ponta a ponta de ingestão de webhooks, carga histórica em massa e
integração event-driven com sistema externo, sobre **SQL Server**, **Apache
Airflow** e **FastAPI**.

> Dados 100% sintéticos, modelados no domínio de um e-commerce de
> eletroportáteis (air fryer, liquidificador, ventilador, linha branca).

---

## Sumário

- [Como rodar](#como-rodar)
- [O que a demo prova](#o-que-a-demo-prova)
- [Arquitetura](#arquitetura)
- [Parte 1 — Modelagem e migrations](#parte-1--modelagem-e-migrations)
- [Parte 2 — Ingestão: FastAPI vs Kafka](#parte-2--ingestão-fastapi-vs-kafka)
- [Parte 3 — Carga de 10 milhões de linhas](#parte-3--carga-de-10-milhões-de-linhas)
- [Parte 4 — Integração event-driven](#parte-4--integração-event-driven)
- [Concorrência, idempotência e memória: o resumo](#concorrência-idempotência-e-memória-o-resumo)
- [Decisões que ficaram de fora](#decisões-que-ficaram-de-fora)

---

## Como rodar

**Pré-requisitos:** Docker com **8 GB de RAM** disponíveis (o SQL Server sozinho
reserva ~2 GB). No Apple Silicon, a imagem do SQL Server é amd64 e roda via
Rosetta — funciona, apenas mais devagar.

```bash
git clone <este-repositorio> && cd ecommerce-integration-challenge
cp .env.example .env
docker compose up -d --build
```

É isso. O serviço `migrate` cria o database, liga o isolamento por snapshot,
aplica as migrations e semeia dados mínimos — e só então as aplicações sobem
(`depends_on: service_completed_successfully`).

A primeira subida leva alguns minutos (build das imagens + inicialização do SQL
Server). Acompanhe com `docker compose logs -f` e confirme que o serviço
`migrate` terminou com `Exited (0)`:

```bash
docker compose ps
```

| Serviço | Endereço | |
|---|---|---|
| Webhook API | http://localhost:8000/docs | ingestão |
| ERP fictício | http://localhost:8001/docs | destino externo |
| Airflow | http://localhost:8080 | `admin` / `admin` |
| SQL Server | `localhost:1433` | `sa` / senha do `.env` |

### Comandos

```bash
# fluxo ponta a ponta, com relatório impresso
docker compose exec webhook-api python -m scripts.demo

# testes unitários (não exigem a stack no ar)
docker compose run --rm --no-deps webhook-api pytest -m "not integration" -q

# testes de integração (exigem a stack no ar)
docker compose exec webhook-api pytest -m integration -q

# lint e checagem de tipos
docker compose run --rm --no-deps webhook-api sh -c "ruff check . && mypy"

# carga na borda de ingestão (429 aqui é o backpressure, não erro)
docker compose exec webhook-api python -m scripts.loadtest --total 50000 --concurrency 128

# BULK INSERT vs fast_executemany vs linha a linha
docker compose exec airflow python /opt/airflow/dags/ecommerce/benchmark.py

# 3 consumidores no mesmo consumer group
docker compose up -d --scale ingest-worker=3

# a carga dos 10 milhões
docker compose exec airflow \
  airflow dags trigger load_order_history_bulk --conf '{"row_count": 10000000}'

# versão curta, para validar o pipeline em minutos
docker compose exec airflow \
  airflow dags trigger load_order_history_bulk --conf '{"row_count": 1000000}'

docker compose logs -f ingest-worker   # ou migrate, dispatch-worker, airflow
docker compose down                    # derruba, mantém os volumes
docker compose down -v                 # derruba e apaga os volumes
```

---

## O que a demo prova

O script de demonstração executa um cenário real e imprime o resultado de cada verificação:

1. **Ingestão** — webhook → Redis Stream → SQL Server.
2. **Idempotência (camadas 1 e 2)** — o mesmo `event_id` reenviado 3x gera **um**
   pedido.
3. **Idempotência (camada 3)** — `event_id` **novo** com o mesmo pedido também
   gera um só. *Este é o caso que derruba quem deduplica apenas por chave de
   evento.*
4. **Integração event-driven** — o pedido aprovado chega ao ERP apesar das
   falhas injetadas, e o número de tentativas é impresso.
5. **Imutabilidade** — um `UPDATE` retroativo no pedido fechado é **recusado
   pelo banco**.
6. **SCD Tipo 2** — o cliente muda de Joinville para Curitiba; o endereço passa a
   ter 2 versões, e o pedido antigo continua apontando para a versão antiga.

---

## Arquitetura

```
  parceiros
     │ HTTP
     ▼
┌─────────────┐   XADD    ┌──────────────┐  XREADGROUP  ┌────────────────┐
│ webhook-api │ ────────▶ │ Redis Stream │ ───────────▶ │ ingest-worker  │
│  FastAPI    │  (lote)   │   (buffer)   │              │   xN (escalável)│
│  202 / 429  │           └──────────────┘              └────────┬───────┘
└─────────────┘                                                  │ mesma transação
                                                                 ▼
                                                   ┌──────────────────────────┐
  ┌─────────────┐   BULK INSERT                    │       SQL Server         │
  │   Airflow   │ ───────────────────────────────▶ │  pedido + outbox (atômico)│
  │  10M linhas │   (volume compartilhado)         └────────────┬─────────────┘
  └─────────────┘                                               │ poll READPAST
                                                                ▼
                                                     ┌──────────────────┐  POST + retry
                                                     │ dispatch-worker  │ ────────────▶ ERP
                                                     └──────────────────┘
```

### Estrutura do código

```
src/
├── shared/          config · logging · domínio (enums, eventos) · db (models, UoW, repos)
│   └── messaging/   EventPublisher (porta) · RedisStreamPublisher · consumer · dedupe
├── webhook_api/     borda HTTP: buffer com batching, backpressure, rotas
├── ingest_worker/   handlers (Strategy) · registry · processor idempotente
├── dispatch_worker/ leitura da outbox (READPAST) · cliente do ERP com retry
└── erp_mock/        ERP fictício com injeção de falhas
migrations/          Alembic (3 revisões)
airflow/dags/        DAG + cliente/ (catálogo, gerador, bulk loader, benchmark)
```

**Padrões aplicados e por quê**

| Padrão | Onde | Por que ali |
|---|---|---|
| **Ports & Adapters** | `EventPublisher` ↔ `RedisStreamPublisher` | é o que torna "trocar Redis por Kafka" uma classe, e não um projeto |
| **Strategy + Registry** | `EventHandler` / `HandlerRegistry` | novo tipo de webhook = nova classe; nenhum `if/elif` cresce no consumidor |
| **Repository** | `src/shared/db/repositories.py` | handlers falam intenção de negócio, não SQL |
| **Unit of Work** | `src/shared/db/uow.py` | torna explícito que aprovar o pedido e gravar na outbox são **uma** transação |
| **Transactional Outbox** | `payment_handler` + `dispatch_worker` | elimina o *dual write* entre banco e HTTP |
| **Application Factory + DI** | `create_app()`, `api/deps.py` | rota testável sem stack no ar |
| **Strategy** | `LoadStrategy` no bulk loader | permite medir as alternativas de carga em vez de opinar sobre elas |

Tipagem com `mypy` (`disallow_untyped_defs`) e lint com `ruff`.

---

## Parte 1 — Modelagem e migrations

Alembic sobre SQLAlchemy 2.0 tipado. Três revisões: schema, triggers de
imutabilidade, tabelas analíticas.

O requisito de negócio esconde **dois problemas diferentes**, e cada um pede um
mecanismo diferente.

### Problema A: o cadastro muda ao longo do tempo

`address` é **SCD Tipo 2**: cada versão do endereço é uma linha própria.
`address_uid` identifica o endereço lógico; `address_id` identifica a versão.
Atualizar = fechar a linha atual (`valid_to`, `is_current = 0`) e inserir outra.

Um **índice único filtrado** garante no banco que existe no máximo uma versão
vigente por endereço lógico:

```sql
CREATE UNIQUE INDEX ux_address_uid_current ON address (address_uid)
WHERE is_current = 1;
```

> **Por que não uma temporal table (system-versioned)?**
> Porque o pedido precisa de **integridade referencial contra a versão exata**
> do endereço, e não se cria FK apontando para uma history table. A temporal
> table daria auditoria de graça, mas não daria a FK — e a FK é o requisito.
> Como a versão precisa ser referenciável, ela tem que ser linha de primeira
> classe, com PK própria. Daí o SCD2.

### Problema B: o pedido fechado não muda retroativamente

Três camadas, da aplicação ao disco:

1. `orders.shipping_address_id` e `billing_address_id` apontam para a **versão**
   do endereço vigente no fechamento. O cliente se muda amanhã e a nota fiscal
   de ontem continua contando a verdade.
2. `order_items` guarda SKU, descrição e preço **copiados** no momento da venda.
   Mudança de catálogo não reescreve o histórico.
3. Um **trigger** recusa `UPDATE`/`DELETE` em colunas financeiras e de vínculo
   quando o status já é terminal (`APPROVED`, `INVOICED`, `CANCELLED`).
   Avançar o **status** continua permitido — é o que diferencia "imutável" de
   "congelado". Outro trigger protege os itens.

O trigger é a última linha de defesa: protege contra qualquer caminho que não
passe pela aplicação — script ad-hoc, job legado, alguém no SSMS.

### Tabelas

`clients` · `address` (SCD2) · `orders` · `order_items` · `payment` ·
`invoices` · `processed_events` (ledger de idempotência) · `outbox` ·
`stg_order_history` (heap) · `fact_order_history` (columnstore).

Na inicialização: **`READ_COMMITTED_SNAPSHOT ON`**. É a configuração mais barata
do projeto — uma linha que faz leitores pararem de bloquear escritores. Sem
ela, todo `SELECT` concorrente entra na fila atrás de quem está gravando.

---

## Parte 2 — Ingestão: FastAPI vs Kafka

**Escolha: Abordagem A (FastAPI), com Redis Streams como broker.**

### A justificativa

O argumento central é simples: **webhook é HTTP por definição, e Kafka não
recebe HTTP.** Escolher "Kafka" para receber webhook de parceiro não elimina a
API — apenas esconde que ela existe, porque alguém teria que colocar um Kafka
REST Proxy na frente, que é uma borda *menos* controlável do que a minha.

A decisão real, portanto, não é *FastAPI ou Kafka*. É **qual broker fica atrás
da borda HTTP**. Posta a pergunta certa:

- **A borda é FastAPI.** É onde eu controlo validação de contrato (Pydantic),
  código de retorno de backpressure (429 + `Retry-After` — a única forma de
  fazer um parceiro externo reduzir o ritmo) e o tempo de resposta. O caminho
  quente **não toca no SQL Server**: se tocasse, a velocidade do banco viraria o
  teto da API e o parceiro tomaria timeout.

- **O broker é Redis Streams.** Consumer groups, ACK explícito, `XAUTOCLAIM`
  para consumidor morto, stream capado (`MAXLEN ~`) como teto de memória.
  Entrega o desacoplamento que o cenário pede com uma fração do footprint
  operacional do Kafka — que, num protótipo que precisa subir com um comando,
  custaria ~1,5 GB de RAM e mais superfície de falha.

### Quando eu trocaria por Kafka

Este não é um "Kafka é overkill" preguiçoso. Os pontos de corte são concretos:

| Trocaria por Kafka quando… | Porque o Redis Streams não entrega |
|---|---|
| for preciso **replay histórico** (reprocessar semanas de eventos) | retenção fica cara na memória |
| houver **vários consumer groups de times diferentes** sobre o mesmo fluxo | opera, mas sem as garantias e o ferramental do Kafka |
| for preciso **ordenação particionada por chave** com garantia forte | Redis Streams é um log único |
| o volume sustentado passar do que um **Redis single-node** aguenta | cluster de Redis Streams é bem mais frágil que cluster Kafka |
| existir exigência de **exactly-once transacional** no broker | Redis não tem transação de produtor |

E é por isso que a interface `EventPublisher` existe: a troca é escrever
`KafkaEventPublisher` e mudar a linha de composição. A borda HTTP não muda.

### Throughput sem timeout e sem estouro de memória

- **Batching** — `asyncio.Queue` + flush por **500 eventos ou 5 ms**, o que vier
  primeiro. 500 webhooks viram **um** round-trip ao Redis, e tráfego baixo não
  paga latência esperando lote encher.
- **Backpressure** — o lag do stream é amostrado a cada 500 ms (medir a cada
  requisição transformaria a proteção em gargalo). Acima do limite: **429 +
  `Retry-After`** proporcional ao atraso. A fila interna tem tamanho máximo; se
  encher, também é 429.
- **Teto de memória** — `MAXLEN ~` no stream e `maxmemory-policy noeviction` no
  Redis: preferimos **recusar** escrita a descartar evento silenciosamente.
  Perder webhook é pior que rejeitar webhook.
- **Shutdown gracioso** — o buffer é drenado antes do processo morrer; deploy
  não perde evento já aceito com 202.

### Idempotência — três camadas, três casos diferentes

| # | Onde | Pega o caso de… |
|---|---|---|
| 1 | Redis `SET NX EX` | duplicata **imediata** — descartada em microssegundos, sem gastar conexão com o banco |
| 2 | PK de `processed_events` | duplicata **tardia**, ou worker que morreu **antes do ACK** e teve a mensagem reentregue |
| 3 | `UNIQUE(order_id)` em `invoices`, `UNIQUE(provider, provider_tx_id)` em `payment` | reenvio com **`event_id` novo** |

A camada 3 é a que importa de verdade para o requisito literal do enunciado —
*"eventos duplicados não podem gerar duplicidade de faturamento"*. As camadas 1
e 2 deduplicam **eventos**; só a 3 garante o **fato de negócio**, porque é uma
constraint, não uma promessa da aplicação. Se um parceiro reenviar o mesmo
pedido com identificador novo, as duas primeiras camadas não veem nada de
errado — o banco vê.

A camada 2 roda **dentro da mesma transação** do efeito de negócio: ou o evento
é registrado como processado e o efeito é gravado, ou nada acontece.

E o detalhe que costuma passar: quando o processamento falha, a reserva no
Redis é **liberada**. Sem isso, um evento que falhou ficaria marcado como
"visto" e o retry seria descartado — perda silenciosa de dado.

### Concorrência e travas no banco

- **Pool limitado, sem overflow** (8 conexões por processo). O número de
  conexões concorrentes é uma constante que **nós escolhemos**, não uma função
  do tráfego. Sob pico, mais carga vira mais espera no pool — não mil conexões
  no SQL Server.
- **`READ_COMMITTED_SNAPSHOT`** — leitores não bloqueiam escritores.
- **Lotes ordenados por chave** antes do INSERT: elimina deadlock por ordem
  inversa de aquisição de lock (worker A pega a linha 1 e quer a 2; worker B
  pega a 2 e quer a 1).
- **Transações curtas** e nada de `MERGE` em tabela quente — `MERGE` é propenso
  a *lock escalation* e a deadlock sob concorrência.
- **ACK só depois do commit**: entrega at-least-once; combinada com
  idempotência, o efeito líquido é exactly-once.

Escale os consumidores com `docker compose up -d --scale ingest-worker=3` —
todos no mesmo consumer group,
cada mensagem entregue a um só.

---

## Parte 3 — Carga de 10 milhões de linhas

DAG `load_order_history_bulk`. Parâmetro `row_count`: **default 10.000.000**, que é
o volume que o desafio pede — o botão *Trigger* na interface do Airflow já
carrega os 10 milhões, sem precisar passar nada.

Para validar o pipeline sem esperar a carga completa, dispare com
`--conf '{"row_count": 1000000}'`: mesmo caminho, um décimo do volume, minutos.

```
prepare_staging ─┐
                 ├─> [generate_and_load] x N ─> load_fact ─> validate_load ─> cleanup
plan_chunks ─────┘        (4 em paralelo)
```

### Requisito de memória

`generate_and_load` usa um **generator**: produz uma linha, o `csv.writer`
escreve, a linha é descartada. **O pico de memória é o mesmo para mil ou para
dez milhões de linhas.**

O que foi evitado, e por quê:
- montar uma lista com todas as linhas → vários GB de RAM;
- usar DataFrame como intermediário → idem, com overhead extra;
- usar Faker linha a linha → ~50x mais lento que sortear de pools
  pré-construídos, sem ganho para dado sintético.

E a prova não é textual: o container do Airflow tem **`mem_limit: 2g`** no
compose. Se o pipeline vazar memória, o container morre e a DAG falha. O
requisito é verificado pelo ambiente, não afirmado no README.

Há um teste unitário que falha se o gerador algum dia virar uma lista
(`test_gerador_e_preguicoso_e_nao_materializa_a_base`).

### Requisito de escrita

**`BULK INSERT` com `TABLOCK`, lendo de um volume compartilhado** entre o worker
do Airflow e o container do SQL Server.

O ponto: **o dado não passa pelo driver Python.** Mandamos um comando e quem lê
o arquivo é o próprio processo do banco. Inserção linha a linha seriam 10
milhões de round-trips; aqui é um por chunk.

Detalhes que somam:
- `TABLOCK` trava a tabela inteira — contraintuitivo, mas é o que habilita
  **minimal logging**; a staging é um heap exclusivo da carga.
- Recovery model **SIMPLE**: o log de transações não cresce sem limite.
- A staging é **heap sem índice nenhum**. Índice durante a carga significaria
  reordenar a estrutura 10 milhões de vezes.
- O **columnstore** da tabela fato é criado vazio e recebe a carga depois
  (compressão ~10x; consulta analítica lê só as colunas necessárias).
- `max_active_tis_per_dag = 4`: paralelismo suficiente para ganhar tempo, baixo
  o bastante para não saturar o SQL Server.
- Cada chunk é uma task: falha isolada, retry barato, sem refazer a carga toda.
- O CSV é apagado assim que carregado (10M de linhas passam de 1 GB em disco), e
  o `cleanup` roda com `trigger_rule="all_done"` — CSV órfão não enche o disco
  nem quando algo falha.

### O pipeline confere o que carregou

`validate_load` falha a DAG se a contagem não bater com o esperado, se houver
campos obrigatórios nulos, e atualiza as estatísticas. Pipeline que carrega e
não confere não carregou — só moveu bytes.

### Benchmark

O script `benchmark.py` mede as três estratégias com o mesmo volume:

| estratégia | observação |
|---|---|
| `BULK INSERT` (volume compartilhado) | referência; o dado não passa pelo Python |
| `pyodbc fast_executemany` | alternativa sem volume compartilhado; ordens de grandeza acima do linha a linha |
| `INSERT` linha a linha | medido com amostra reduzida — medi-lo com 10M levaria horas, que é exatamente o motivo de o enunciado desclassificá-lo |

O script imprime linhas/s e a projeção para 10M, **declarando que a projeção dos
métodos lentos é extrapolação linear**. A medição real dos 10M é a própria DAG.

> Os números dependem da máquina. Em Apple Silicon, o SQL Server roda emulado
> via Rosetta e os valores caem — vale registrar em qual ambiente foram medidos.

---

## Parte 4 — Integração event-driven

### O problema que o Outbox resolve

Quando o pagamento é aprovado, duas coisas precisam acontecer: gravar no banco e
avisar o ERP por HTTP. **Elas não são atômicas.**

- commit OK + POST falhou → o ERP nunca soube do pedido;
- POST OK + commit falhou → o ERP conhece um pedido que não existe.

É o *dual write*, e nenhum retry resolve, porque o problema é a fronteira
transacional, não a rede.

**A solução é não chamar HTTP nessa hora.** O `PaymentUpdatedHandler` grava o
pagamento, muda o pedido para `APPROVED` e insere a mensagem na `outbox` — tudo
na **mesma transação**. Ou as três acontecem, ou nenhuma. O `dispatch-worker`
lê a outbox depois e faz o POST com calma.

### Por que a outbox e não um poll na tabela `orders`

O enunciado pede um worker que "monitore os novos pedidos inseridos". A leitura
literal seria varrer `orders WHERE status = 'APPROVED'` por timestamp ou por um
flag `enviado`. Três motivos para não fazer assim:

1. **Janela de perda.** Marcar `enviado = 1` depois do POST é um segundo commit;
   se o processo morre entre o POST e esse commit, o pedido é reenviado — e se a
   ordem for invertida, nunca é enviado. A outbox resolve porque a intenção de
   enviar nasce na mesma transação do fato que a origina.
2. **Contenção na tabela quente.** `orders` é lida e escrita pela ingestão o
   tempo todo. Varrê-la de segundo em segundo com um worker de saída coloca dois
   caminhos concorrentes na mesma tabela. A `outbox` é uma fila dedicada, com
   índice filtrado só sobre o que está pendente.
3. **Estado de entrega não é estado do pedido.** Tentativas, último erro,
   próxima tentativa e DLQ são atributos da *entrega*, não do pedido. Guardá-los
   em `orders` polui a tabela de negócio com detalhe de infraestrutura.

O efeito observável é o mesmo que o enunciado descreve — pedido vira `APPROVED`,
o ERP recebe o payload completo —, mas sem a janela de inconsistência.

O payload completo (cliente + **endereço na versão referenciada pelo pedido** +
itens + pagamento) é montado **no momento da aprovação** e congelado na outbox —
o que mantém a coerência com a regra de imutabilidade da Parte 1.

### Ciclo de vida de uma mensagem

```
PENDING --claim(READPAST)--> IN_FLIGHT --POST ok--> SENT
                                  │
                                  ├── falha temporária ──> PENDING (backoff)
                                  └── tentativas esgotadas / 4xx ──> DLQ
```

- **`READPAST`** no poll: o `UPDATE ... OUTPUT` marca e devolve as linhas num
  comando atômico, e workers concorrentes **pulam** as linhas travadas por
  outro em vez de esperar. É o que permite N workers na mesma fila sem fila
  indiana — sem ele, cinco workers teriam a vazão de um.
- **Backoff exponencial com jitter.** O jitter não é enfeite: sem ele, todas as
  mensagens pendentes voltam no mesmo instante e derrubam o ERP de novo assim
  que ele levanta.
- **`Idempotency-Key`** estável entre tentativas: reenviar nunca cria dois
  pedidos no ERP. Damos ao ERP a mesma cortesia que esperamos dos parceiros.
- **DLQ** com o último erro preservado: o que falhou não se perde e não trava a
  fila dos saudáveis.
- **Mensagens presas** em `IN_FLIGHT` por um worker que morreu no meio do POST
  são liberadas periodicamente.
- Distinção entre **falha temporária** (5xx, timeout → retry) e **permanente**
  (4xx de contrato → DLQ direto; reenviar não vai melhorar).

O `erp-mock` injeta falhas numa taxa configurável (`ERP_FAILURE_RATE`, default
25%) — metade erro de servidor, metade timeout. A resiliência é
**demonstrável**, não alegada: a demo imprime quantas tentativas foram
necessárias.

---

## Concorrência, idempotência e memória: o resumo

Se o avaliador ler só esta seção:

**Idempotência** — três camadas independentes: dedupe rápido no Redis, PK no
ledger `processed_events` dentro da transação de negócio, e constraints
`UNIQUE` que tornam duplicidade de faturamento **impossível no banco**, não
apenas improvável na aplicação. ACK no stream só depois do commit.

**Concorrência** — pool de conexões fixo (o tráfego não define quantas conexões
o banco recebe), `READ_COMMITTED_SNAPSHOT` para leitores não bloquearem
escritores, lotes ordenados para eliminar deadlock por ordem inversa,
transações curtas, `READPAST` na fila da outbox para workers não se
bloquearem, e backpressure explícito (429) devolvido ao parceiro quando os
consumidores ficam para trás.

**10 milhões de linhas** — generator com memória O(1), CSV em volume
compartilhado, `BULK INSERT` com `TABLOCK` onde o dado não atravessa o driver
Python, chunks paralelos com limite, índices depois da carga, columnstore no
fato, e uma task de validação que falha a DAG se a contagem não bater. O
`mem_limit: 2g` no container transforma o requisito de memória em teste.

---

## Decisões que ficaram de fora

Cortadas conscientemente — um protótipo que precisa subir com um comando paga
caro por peça a mais:

- **Temporal table no `clients`** — SCD2 já resolve o histórico. Dois mecanismos
  de versionamento no mesmo schema seriam confusão, não profundidade.
- **Circuit breaker** — backoff com jitter, teto de tentativas e DLQ já cobrem o
  requisito de resiliência. Em produção com ERP real, seria a primeira adição.
- **Table swap com `sp_rename`** — `INSERT ... SELECT` com `TABLOCK` resolve
  neste volume.
- **Métricas Prometheus e validação HMAC das assinaturas** — não pedidos; em
  produção, ambos entrariam antes de qualquer outra coisa.
- **Kafka** — pelos motivos da seção da Parte 2, com os pontos de corte
  explicitados.

## Notas de ambiente

- **Apple Silicon**: `platform: linux/amd64` no serviço `mssql` (Rosetta).
  Funciona; benchmarks ficam mais lentos que em hardware amd64 nativo.
- O `sa` é usado por simplicidade do protótipo — `BULK INSERT` exige
  `ADMINISTER BULK OPERATIONS`. Em produção, usuário dedicado com permissão
  mínima.
- O `erp-mock` guarda o que recebeu em memória: é um mock, não um sistema.
