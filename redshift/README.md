# Redshift - Simulação do Amazon Redshift (Data Warehouse)

## O que é o Amazon Redshift?

**Amazon Redshift** é o Data Warehouse gerenciado da AWS, otimizado para
analytics em escala de petabytes.

### Por que é diferente do PostgreSQL comum?

| Característica    | PostgreSQL (OLTP)          | Redshift (OLAP/DW)           |
|-------------------|----------------------------|------------------------------|
| Armazenamento     | Por linha (row-based)      | Por coluna (columnar)        |
| Uso ideal         | CRUD, transações           | Analytics, agregações        |
| Query típica      | SELECT * WHERE id=123      | SELECT SUM, GROUP BY         |
| Escala            | Gigabytes                  | Petabytes                    |
| Custo             | EC2 + EBS                  | $0.25/hora/nó                |
| Compressão        | Limitada                   | Automática por coluna        |

### Armazenamento Colunar Explicado

```
ARMAZENAMENTO POR LINHA (PostgreSQL):
┌─────────────────────────────────────────────────────────┐
│ linha 1: [id=1][nome=João][valor=100][data=2024-01-01]   │
│ linha 2: [id=2][nome=Maria][valor=200][data=2024-01-02]  │
│ linha 3: [id=3][nome=Pedro][valor=150][data=2024-01-03]  │
└─────────────────────────────────────────────────────────┘
Query: SELECT SUM(valor) → lê TUDO (id, nome, valor, data)

ARMAZENAMENTO COLUNAR (Redshift):
┌───────────┐ ┌────────────────────┐ ┌──────────────────┐
│ id:       │ │ nome:              │ │ valor:           │
│ [1,2,3,…] │ │ [João,Maria,Pedro] │ │ [100,200,150,…]  │
└───────────┘ └────────────────────┘ └──────────────────┘
Query: SELECT SUM(valor) → lê APENAS coluna valor (10-100x mais rápido!)
```

## Conceitos Avançados do Redshift

### Distribution Keys (DISTKEY)
Define como os dados são distribuídos entre os nós do cluster.

```sql
-- Distribui pela coluna cpf_cliente
-- Todas as vendas do mesmo cliente ficam no MESMO nó
-- Perfeito para JOINs frequentes pelo cpf_cliente
CREATE TABLE fato_vendas (
    id_venda   BIGINT,
    cpf_cliente VARCHAR(11) DISTKEY,
    valor_total DECIMAL(15,2)
);
```

Tipos de distribuição:
- `DISTKEY`: Distribui por hash de uma coluna
- `ALL`: Cópia completa em cada nó (tabelas de dimensão pequenas)
- `EVEN`: Distribui igualmente (round-robin)
- `AUTO`: Redshift decide automaticamente

### Sort Keys (SORTKEY)
Define a ordem física dos dados no disco. Similar a índice, mas diferente.

```sql
-- Dados ordenados por data_venda
-- Queries com WHERE data_venda BETWEEN ... são muito rápidas
-- Redshift pula blocos de disco irrelevantes (zone maps)
CREATE TABLE fato_vendas (
    data_venda DATE SORTKEY,
    valor_total DECIMAL(15,2)
);
```

### VACUUM e ANALYZE
Operações de manutenção necessárias após grandes INSERTs/DELETEs:
```sql
-- VACUUM: reorganiza dados deletados e aplica sort keys
VACUUM SORT ONLY dw.fato_vendas;

-- ANALYZE: atualiza estatísticas para o query planner
ANALYZE dw.fato_vendas;
```

## Arquitetura do Data Warehouse

```
staging/            → Dados brutos copiados do S3 (zona de aterrissagem)
   stg_clientes
   stg_vendas
       ↓
dw/ (Star Schema)  → Modelo dimensional otimizado
   dim_clientes     → Dimensão: atributos dos clientes
   dim_produto      → Dimensão: atributos dos produtos
   dim_tempo        → Dimensão: calendário completo
   fato_vendas      → Fato: métricas de vendas
       ↓
dm/ (Data Marts)   → Agregações pré-calculadas para BI
   dm_vendas_diarias
   dm_vendas_canal
   dm_top_clientes
```

## Star Schema (Esquema Estrela)

O modelo mais usado em Data Warehouses:

```
                    dim_tempo
                    (calendário)
                         │
                         │ FK: id_tempo
                         │
dim_clientes ────── fato_vendas ────── dim_produto
(quem comprou)     (o que aconteceu)   (o que foi comprado)
    id_cliente FK       id_venda
    nome                data_venda
    uf                  valor_total
                        quantidade
                        FK: id_cliente
                        FK: id_produto
                        FK: id_tempo
```

## Comandos Específicos do Redshift

```sql
-- COPY: carrega dados do S3 para o Redshift (muito mais rápido que INSERT)
COPY dw.fato_vendas
FROM 's3://bucket/processed/vendas/'
IAM_ROLE 'arn:aws:iam::123456789:role/redshift-role'
FORMAT AS PARQUET;

-- UNLOAD: exporta do Redshift para o S3
UNLOAD ('SELECT * FROM dw.fato_vendas WHERE data_venda > ''2024-01-01''')
TO 's3://bucket/exports/vendas/'
IAM_ROLE 'arn:aws:iam::123456789:role/redshift-role'
FORMAT AS PARQUET;
```

## Simulação Local

Como não temos Redshift, usamos **PostgreSQL**.
O SQL é 95% compatível - aprendendo aqui, você usa no Redshift real.

Diferenças que você vai notar na migração:
- DISTKEY e SORTKEY: ignorados no PostgreSQL, funcionais no Redshift
- COPY S3: no PostgreSQL, usamos `\COPY` ou psycopg2
- ENCODE: compressão colunar (não existe no PostgreSQL)
- SVL_QUERY_SUMMARY: view de análise de queries (só no Redshift)
