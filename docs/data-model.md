# Modelo de dados

```mermaid
erDiagram
    clients ||--o{ address : "possui versoes"
    clients ||--o{ orders : "faz"
    address ||--o{ orders : "versao usada no fechamento"
    orders  ||--|{ order_items : "contem"
    orders  ||--o{ payment : "recebe"
    orders  ||--o| invoices : "gera (1:1)"

    clients {
        bigint  client_id PK
        varchar client_uid UK
        nvarchar full_name
        varchar document
        varchar email
    }

    address {
        bigint   address_id PK "identifica a VERSAO"
        varchar  address_uid   "identifica o endereco logico"
        bigint   client_id FK
        nvarchar street
        varchar  number
        nvarchar city
        char     state
        varchar  zip_code
        datetime valid_from
        datetime valid_to     "NULL na versao vigente"
        bit      is_current   "unique filtrado por address_uid"
    }

    orders {
        bigint   order_id PK
        varchar  order_uid UK
        bigint   client_id FK
        bigint   shipping_address_id FK "VERSAO do endereco"
        bigint   billing_address_id FK
        varchar  status "PENDING|APPROVED|INVOICED|CANCELLED"
        decimal  total_amount
        datetime placed_at
        datetime closed_at
    }

    order_items {
        bigint   order_item_id PK
        bigint   order_id FK
        varchar  sku          "copiado na venda"
        nvarchar description  "copiado na venda"
        int      quantity
        decimal  unit_price   "copiado na venda"
        decimal  line_total
    }

    payment {
        bigint   payment_id PK
        bigint   order_id FK
        varchar  provider
        varchar  provider_tx_id "UK(provider, provider_tx_id)"
        varchar  method "PIX|CREDIT_CARD|BOLETO"
        varchar  status
        decimal  amount
        datetime approved_at
    }

    invoices {
        bigint   invoice_id PK
        bigint   order_id FK "UNIQUE -> 1 fatura por pedido"
        varchar  invoice_number
        varchar  series
        varchar  access_key
        datetime issued_at
        decimal  total_amount
    }
```

## Tabelas de infraestrutura

```mermaid
erDiagram
    processed_events {
        varchar  event_id PK "chave de idempotencia de ponta a ponta"
        varchar  event_type
        varchar  payload_hash
        datetime processed_at
    }

    outbox {
        bigint   outbox_id PK
        varchar  aggregate_id "order_uid"
        varchar  event_type
        nvarchar payload "snapshot completo do pedido"
        varchar  status "PENDING|IN_FLIGHT|SENT|DLQ"
        int      attempts
        datetime next_attempt_at
        varchar  locked_by
        nvarchar last_error
    }
```

## Analiticas (Parte 3)

| Tabela | Estrutura | Papel |
|---|---|---|
| `stg_order_history` | heap, **sem indice** | alvo do `BULK INSERT`; indice durante a carga custaria reordenacao 10M de vezes |
| `fact_order_history` | **clustered columnstore** | compressao ~10x; consulta analitica le so as colunas necessarias |

## As duas invariantes centrais

1. **Uma versao vigente por endereco logico**
   `CREATE UNIQUE INDEX ux_address_uid_current ON address (address_uid) WHERE is_current = 1`

2. **Uma fatura por pedido**
   `UNIQUE (order_id)` em `invoices` — e por isso que "evento duplicado nao gera
   faturamento duplicado" e uma garantia do banco, nao uma promessa da aplicacao.
