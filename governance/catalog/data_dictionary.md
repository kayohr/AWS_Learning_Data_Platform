# Dicionário de Dados - Plataforma de Dados

## Tabela: staging.stg_clientes / dw.dim_clientes

| Coluna           | Tipo        | Obrigatório | Descrição                                    | Exemplo              |
|------------------|-------------|-------------|----------------------------------------------|----------------------|
| cpf              | VARCHAR(11) | Sim         | CPF sem formatação, 11 dígitos numéricos      | `12345678901`        |
| nome             | VARCHAR(200)| Sim         | Nome completo, Title Case                    | `João da Silva`      |
| email            | VARCHAR(200)| Não         | E-mail válido (validado por regex)            | `joao@email.com`     |
| telefone         | VARCHAR(20) | Não         | Formato: (XX) XXXXX-XXXX                     | `(11) 98765-4321`    |
| data_nascimento  | DATE        | Não         | Formato: YYYY-MM-DD                          | `1990-05-15`         |
| cidade           | VARCHAR(100)| Não         | Nome da cidade                               | `São Paulo`          |
| uf               | CHAR(2)     | Não         | Sigla do estado (2 letras maiúsculas)        | `SP`                 |
| segmento_cliente | VARCHAR(10) | Não         | B2B (empresa) ou B2C (pessoa física)         | `B2C`                |
| limite_credito   | DECIMAL     | Não         | Valor em R$ do limite de crédito             | `5000.00`            |

**Chave de negócio**: `cpf`
**Dados sensíveis (LGPD)**: cpf, nome, email, telefone, data_nascimento

## Tabela: staging.stg_vendas / dw.fato_vendas

| Coluna          | Tipo          | Obrigatório | Descrição                                      | Exemplo            |
|-----------------|---------------|-------------|------------------------------------------------|--------------------|
| id_venda        | BIGINT        | Sim         | ID único da venda no ERP                       | `12345`            |
| cpf_cliente     | VARCHAR(11)   | Sim         | CPF do cliente (FK para dim_clientes)          | `12345678901`      |
| data_venda      | TIMESTAMP     | Sim         | Data e hora da venda                           | `2024-01-15 14:30` |
| valor_total     | DECIMAL(15,2) | Sim         | Valor total em R$ (positivo)                   | `299.90`           |
| quantidade      | INTEGER       | Sim         | Quantidade de itens (>= 1)                     | `2`                |
| canal_venda     | VARCHAR(30)   | Não         | ECOMMERCE, LOJA_FISICA, TELEVENDAS, APP_MOBILE | `ECOMMERCE`        |
| status_venda    | VARCHAR(20)   | Não         | CONCLUIDA, CANCELADA, DEVOLVIDA, PENDENTE      | `CONCLUIDA`        |
| forma_pagamento | VARCHAR(30)   | Não         | CARTAO_CREDITO, PIX, BOLETO, CARTAO_DEBITO     | `PIX`              |

**Chave de negócio**: `id_venda`
**Granularidade**: 1 linha = 1 venda (1 pedido completo)

## Regras de Negócio

1. `valor_total` deve ser > 0 (nunca negativo exceto estorno)
2. `cpf_cliente` deve existir em `dim_clientes` (integridade referencial)
3. Vendas CANCELADAS não contam para métricas de receita
4. `data_venda` nunca pode ser futura
5. `quantidade` >= 1 (mínimo um item)

## Lineage (Linhagem dos Dados)

```
CRM PostgreSQL → Airbyte → s3/raw/clientes/ → Glue Job → s3/processed/clientes/ → staging.stg_clientes → dw.dim_clientes → dm.dm_top_clientes
API Vendas     → Airbyte → s3/raw/vendas/   → Glue Job → s3/processed/vendas/   → staging.stg_vendas   → dw.fato_vendas  → dm.dm_vendas_diarias
```

## Retenção de Dados (LGPD)

| Zona         | Retenção | Após expirar       |
|--------------|----------|--------------------|
| raw/         | 90 dias  | Mover para archived|
| processed/   | 180 dias | Mover para archived|
| archived/    | 2 anos   | Deletar            |
| DW (dw.*)    | 5 anos   | Anonimizar CPF/nome|
| Data Marts   | 3 anos   | Deletar            |
