# Pipeline de integração em larga escala

Protótipo de ingestão de webhooks, carga de histórico e integração com sistema
externo, usando FastAPI, Redis Streams, Apache Airflow e SQL Server. Todos os
dados são sintéticos.

## Sumário

- [Como rodar](#como-rodar)
- [Como testar](#como-testar)
- [FastAPI ou Kafka](#fastapi-ou-kafka)
- [Concorrência](#concorrência)
- [Idempotência](#idempotência)
- [A carga de 10 milhões](#a-carga-de-10-milhões)

## Como rodar

Requisitos: Docker com Compose v2, 8 GB de memória liberados (o SQL Server
reserva cerca de 2 GB sozinho) e as portas 1433, 6379, 8000, 8001 e 8080 livres.

```bash
git clone https://github.com/viniciusdevlacerda/ecommerce-integration-challenge.git
cd ecommerce-integration-challenge
./run.sh
```

O `run.sh` verifica o ambiente, cria o `.env`, sobe os containers, espera as
migrations e a API, e roda a demonstração. Se algo falhar, ele mostra o log do
serviço responsável. O equivalente sem o script é `cp .env.example .env` seguido
de `docker compose up -d --build`.

O serviço `migrate` cria o database, habilita `READ_COMMITTED_SNAPSHOT`, aplica
as migrations do Alembic e insere alguns registros. Os demais serviços só sobem
depois que ele termina, porque declaram
`depends_on: migrate: service_completed_successfully`.

A primeira execução leva de 5 a 10 minutos: as imagens instalam o driver ODBC da
Microsoft e o SQL Server demora cerca de 40 segundos para aceitar a primeira
conexão. Depois disso a stack sobe em segundos.

Para conferir, `docker compose ps` deve mostrar o `migrate` em `Exited (0)` e os
outros em `running` ou `healthy`. Se ele sair com outro código,
`docker compose logs migrate` mostra o motivo.

| Serviço | Endereço | Acesso |
|---|---|---|
| API de webhooks | http://localhost:8000/docs | |
| ERP fictício | http://localhost:8001/docs | `GET /erp/orders` lista o que ele recebeu |
| Airflow | http://localhost:8080 | admin / admin |
| SQL Server | localhost:1433 | usuário `sa`, senha do `.env`, base `ecommerce_ops` |

## Como testar

```bash
./run.sh demo      # fluxo ponta a ponta
./run.sh verify    # resumo do estado do banco e da fila
./run.sh test      # testes unitários e de integração
./run.sh load      # carga na API de ingestão
./run.sh dag       # carga de 10 milhões de linhas
./run.sh bench     # compara as estratégias de escrita no SQL Server
./run.sh logs ingest-worker
./run.sh down
```

A demonstração envia eventos de cliente, pedido e pagamento, reenvia duplicatas
de propósito, espera o pedido chegar ao ERP, tenta alterar um pedido já fechado e
muda o endereço do cliente. Cada verificação imprime o resultado. Duas linhas
valem atenção: `tentativas até entregar`, que mostra o worker de saída vencendo
as falhas que o ERP fictício injeta (25% das chamadas, configurável em
`ERP_FAILURE_RATE`), e `endereço DO PEDIDO`, que continua Joinville mesmo depois
de o cliente mudar para Curitiba.

O `verify` imprime as contagens por tabela, quantos endereços já foram
versionados, o estado da outbox e as verificações de duplicidade, que devem estar
todas em zero.

No teste de carga, respostas 429 são esperadas: indicam o backpressure atuando,
não falha.

A carga em massa também pode ser disparada pelo botão *Trigger* na interface do
Airflow, na DAG `load_order_history_bulk`, já com os 10 milhões. Para validar o
caminho mais rápido, `./run.sh dag 1000000`.

## FastAPI ou Kafka

Escolhi a Abordagem A, FastAPI, com Redis Streams como fila atrás dela.

O primeiro ponto que pesou é que webhook é HTTP e Kafka não recebe HTTP. Escolher
a Abordagem B não elimina a necessidade de uma API na frente: alguém teria que
expor um endpoint para o parceiro chamar e produzir no tópico. Na prática isso
seria um Kafka REST Proxy, que é uma borda com menos controle do que a que eu
escreveria. A decisão que realmente existe é qual fila fica atrás dessa API.

Com a pergunta nesses termos, a API em FastAPI é onde eu quero decidir três
coisas que o Kafka não decide por mim:

1. o contrato de entrada, validado pelo Pydantic antes de qualquer coisa tocar a
   fila;
2. o código de resposta quando o sistema está sobrecarregado. Devolver 429 com
   `Retry-After` é a única forma de pedir a um parceiro externo que reduza o
   ritmo, e isso é HTTP, não broker;
3. o tempo de resposta. A rota responde 202 sem tocar o banco, o que mantém a
   latência na casa dos milissegundos independentemente de quanto o SQL Server
   está aguentando naquele momento.

Para a fila em si, Redis Streams entrega o que o cenário pede: consumer groups
com entrega exclusiva por mensagem, ACK explícito, `XAUTOCLAIM` para recuperar o
que ficou pendente em um consumidor que morreu, e limite de tamanho no stream
via `MAXLEN`. Tudo isso com uma fração do custo operacional de subir e manter um
cluster Kafka, o que importa num protótipo que precisa subir com um comando.

Kafka seria a escolha melhor em quatro situações concretas:

* **Replay histórico.** Se for preciso reprocessar semanas de eventos, a retenção
  longa do Kafka em disco é barata e a do Redis, que vive em memória, não é.
* **Vários consumidores independentes.** Quando times diferentes consomem o mesmo
  fluxo com offsets próprios, o ferramental do Kafka é bem mais maduro.
* **Ordenação particionada com garantia forte.** Redis Streams é um log único;
  particionar por chave com garantia de ordem dentro da partição é nativo no
  Kafka.
* **Volume acima do que um Redis single-node aguenta.** Clusterizar Redis Streams
  é bem mais frágil do que crescer um cluster Kafka.

A troca custa uma classe. A API depende de `EventPublisher`, um Protocol em
`src/shared/messaging/ports.py` sem nenhuma dependência de infraestrutura.
`RedisStreamPublisher` é a implementação atual; uma `KafkaEventPublisher` entraria
no lugar sem alterar rota, buffer ou backpressure.

## Concorrência

O risco que o desafio aponta, travar o banco por excesso de conexões ou de locks,
é tratado em quatro pontos.

**O pool de conexões é fixo e sem overflow.** `pool_size=8` e `max_overflow=0` em
`src/shared/db/engine.py`. Com isso o número de conexões simultâneas no SQL
Server é uma constante que eu escolho, e não uma função do tráfego de entrada.
Sob pico, o excedente espera no pool do lado da aplicação. Com overflow habilitado
um pico de 1000 requisições por segundo viraria centenas de conexões no banco, que
é exatamente o modo de falha que se quer evitar.

**Leitores não bloqueiam escritores.** O bootstrap liga
`READ_COMMITTED_SNAPSHOT` no database. Sem isso, qualquer SELECT concorrente
espera quem está gravando na mesma linha. É uma linha de configuração que remove
a maior parte da contenção antes de ela aparecer.

**Os lotes são ordenados antes de gravar.** Deadlock em inserção concorrente
costuma vir de ordem inversa de aquisição de lock: um worker pega a linha A e
quer a B, outro pegou a B e quer a A. Processando cada lote em ordem estável de
chave, essa situação não se forma. Erro 1205 ainda é tratado com retry e backoff,
mas passa a ser exceção.

**A fila de saída usa `READPAST`.** O worker que despacha para o ERP reserva
mensagens com `UPDATE TOP (n) ... WITH (ROWLOCK, UPDLOCK, READPAST) OUTPUT
inserted.*`, em `src/dispatch_worker/outbox_reader.py`. O `UPDLOCK` evita corrida
entre a leitura e a marcação, e o `READPAST` faz um worker pular as linhas que
outro já travou em vez de esperar por elas. Sem isso, cinco workers na mesma fila
teriam a vazão de um.

Do lado da entrada, o backpressure fecha o ciclo. A API mede o atraso da fila a
cada 500 ms (medir a cada requisição transformaria a proteção em gargalo) e, acima
do limite, responde 429 com `Retry-After` proporcional. O buffer interno também
tem tamanho máximo; se encher, a resposta é a mesma. O stream do Redis tem
`MAXLEN` e o servidor está com `maxmemory-policy noeviction`, ou seja, prefere
recusar escrita a descartar evento em silêncio.

## Idempotência

São três barreiras independentes, em `src/ingest_worker/processor.py` e no
schema. Cada uma cobre um caso que as outras não cobrem.

**Reserva no Redis.** `SET evt:<event_id> NX EX 86400`. Se a chave já existe, o
evento é duplicata e o processamento para ali, sem gastar uma conexão com o
banco. Cobre o caso mais comum, que é o parceiro reenviando em poucos segundos.
Quando o processamento falha, a reserva é liberada; sem isso, um evento que deu
erro ficaria marcado como visto e a reentrega seria descartada em silêncio.

**Chave primária em `processed_events`.** O `event_id` é gravado nessa tabela
dentro da mesma transação do efeito de negócio. Violação de PK significa que o
evento já foi processado. Cobre o que a primeira barreira não pega: duplicata que
chega horas depois, Redis reiniciado, e o caso de o worker morrer entre o commit e
o ACK no stream, o que faz a mensagem ser reentregue.

**Constraints de negócio.** `UNIQUE (order_id)` em `invoices` e
`UNIQUE (provider, provider_tx_id)` em `payment`. Essas cobrem o caso que derruba
quem deduplica só por chave de evento: o parceiro reenviando o mesmo fato com um
`event_id` novo. Para as duas primeiras barreiras é um evento inédito. Para o
banco, é uma segunda fatura do mesmo pedido, e ele recusa. É o que transforma
"evento duplicado não gera faturamento duplicado" em garantia estrutural em vez
de promessa da aplicação.

Duas decisões completam o conjunto.

O ACK no Redis só acontece depois do commit no SQL Server. A entrega passa a ser
at-least-once, e como o processamento é idempotente o efeito observável é
exactly-once. A ordem inversa seria mais simples e perderia eventos em qualquer
queda entre o ACK e o commit.

Na aprovação do pagamento, o handler grava o pagamento, muda o pedido para
APPROVED e insere a mensagem na outbox na mesma transação. A transição de status
devolve falso se o pedido já estava aprovado, então um `payment.approved`
duplicado não gera um segundo despacho para o ERP. E o `Idempotency-Key` enviado
no POST é derivado do id da mensagem na outbox, estável entre tentativas, de modo
que o retry não cria pedido repetido do outro lado.

## A carga de 10 milhões

A DAG é `load_order_history_bulk`, em `airflow/dags/load_order_history_dag.py`.
O parâmetro `row_count` tem 10 milhões como padrão.

O fluxo é: preparar a staging, planejar os chunks, gerar e carregar cada chunk em
paralelo, consolidar na tabela fato, validar e limpar os arquivos.

### Memória

O gerador em `airflow/dags/ecommerce/generator.py` é um generator. Ele produz uma
linha, o `csv.writer` escreve, a linha é descartada. O consumo de memória é o
mesmo para mil ou para dez milhões de linhas: medi cerca de 2 MB acima da linha
de base em ambos os casos, gerando a 110 mil linhas por segundo.

O que eu evitei, e por quê:

* montar uma lista com todas as linhas antes de gravar consumiria vários GB;
* usar DataFrame como passo intermediário tem o mesmo problema, com overhead
  adicional;
* gerar cada campo com Faker é cerca de cinquenta vezes mais lento que sortear de
  listas pré-construídas, sem ganho nenhum para dado sintético.

O container do Airflow está com `mem_limit: 2g` no compose. Se o processo vazar
memória, ele morre e a DAG falha. O requisito passa a ser verificado pelo
ambiente em vez de afirmado aqui. Há também um teste unitário que quebra se o
gerador deixar de ser preguiçoso.

### Escrita

Cada chunk vira um CSV em um volume compartilhado entre o container do Airflow e
o do SQL Server. A carga é feita com:

```sql
BULK INSERT stg_order_history
FROM '/var/opt/mssql/bulk/order_history_0000.csv'
WITH (FORMAT = 'CSV', FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a',
      FIRSTROW = 2, BATCHSIZE = 100000, TABLOCK, MAXERRORS = 0)
```

O ganho vem de o arquivo ser lido pelo próprio processo do SQL Server. Os dados
não atravessam o driver Python. Inserção linha a linha seriam dez milhões de
idas e voltas de rede; aqui é uma por chunk.

Três detalhes acompanham:

* `TABLOCK` trava a tabela inteira, o que habilita minimal logging. A staging é
  um heap exclusivo da carga, ninguém mais a usa durante o processo.
* O database está em recovery model SIMPLE, então o log de transações não cresce
  sem limite durante a carga.
* A staging não tem índice nenhum. Manter índice durante a inserção significaria
  reordenar a estrutura dez milhões de vezes. A tabela fato recebe um clustered
  columnstore, criado antes da carga porque carregar em columnstore vazio é
  rápido e a compressão fica em torno de dez vezes.

Existe uma segunda estratégia implementada, `fast_executemany` do pyodbc, em
`airflow/dags/ecommerce/bulk_loader.py`. Ela é selecionável por parâmetro da DAG
e serve para ambientes sem volume compartilhado. É mais lenta que o BULK INSERT,
porque os dados passam pelo driver, mas continua ordens de grandeza acima de
inserção linha a linha.

### Paralelismo e verificação

O planejamento divide o volume em chunks de 500 mil linhas, o que dá 20 tasks
mapeadas dinamicamente. `max_active_tis_per_dag = 4` limita quantas carregam ao
mesmo tempo: paralelismo suficiente para reduzir o tempo total sem saturar o SQL
Server nem o disco do worker. Cada chunk é uma task independente, então uma falha
isolada é reprocessada sozinha, sem refazer a carga inteira. O CSV é apagado logo
após a carga, e a limpeza final roda com `trigger_rule="all_done"` para não
deixar arquivo órfão ocupando disco quando algo falha no meio.

A última etapa compara a contagem da tabela fato com o volume pedido, verifica
campos obrigatórios nulos e atualiza as estatísticas. Se a contagem não bater, a
DAG falha.

O script `benchmark.py` mede BULK INSERT, `fast_executemany` e inserção linha a
linha com o mesmo volume e imprime linhas por segundo. A medição do método linha
a linha usa uma amostra reduzida e o resultado é extrapolado, o que o script
declara na saída. Medi-lo com dez milhões levaria horas, que é justamente o
motivo de ele estar descartado.
