# Documentação — Plataforma de Dados Local (Simulação AWS)

> Projeto de estudo que simula uma plataforma de dados completa da AWS rodando localmente,
> cobrindo as responsabilidades de um Engenheiro de Dados: ETL/ELT, Data Warehouse,
> streaming, qualidade, governança e monitoramento.

---

## Índice

1. [O que é este projeto?](#1-o-que-é-este-projeto)
2. [Arquitetura geral](#2-arquitetura-geral)
3. [Mapeamento Local → AWS](#3-mapeamento-local--aws)
4. [Fluxo de dados passo a passo](#4-fluxo-de-dados-passo-a-passo)
5. [Componentes detalhados](#5-componentes-detalhados)
   - [S3 — Data Lake](#51-s3--data-lake)
   - [Glue — ETL](#52-glue--etl)
   - [Lambda — Funções event-driven](#53-lambda--funções-event-driven)
   - [SQS — Filas de mensagens](#54-sqs--filas-de-mensagens)
   - [Redshift — Data Warehouse](#55-redshift--data-warehouse)
   - [Airflow — Orquestração](#56-airflow--orquestração)
   - [Airbyte — Ingestão de fontes externas](#57-airbyte--ingestão-de-fontes-externas)
   - [Streaming — Kafka / Jitsu](#58-streaming--kafka--jitsu)
   - [Data Quality](#59-data-quality)
   - [Governance — Governança](#510-governance--governança)
   - [Monitoring — Monitoramento](#511-monitoring--monitoramento)
6. [Estrutura de pastas](#6-estrutura-de-pastas)
7. [Como rodar o projeto](#7-como-rodar-o-projeto)
8. [Dados gerados](#8-dados-gerados)
9. [Glossário](#9-glossário)

---

## 1. O que é este projeto?

### O problema que resolve

Uma empresa de e-commerce tem dados espalhados em vários lugares:
- Clientes no sistema CRM
- Vendas no ERP
- Cliques e comportamento no site
- Catálogo de produtos em planilhas CSV

Para tomar decisões (relatórios, BI, machine learning), precisa **centralizar, organizar e transformar** esses dados num único lugar confiável.

Esse projeto simula exatamente essa estrutura — localmente, sem pagar nada na AWS.

### O que você aprende aqui

```
┌─────────────────────────────────────────────────────────────────┐
│  Habilidade                         Onde está no projeto        │
├─────────────────────────────────────────────────────────────────┤
│  Ingestão de dados (batch)          airbyte/                    │
│  Ingestão de dados (tempo real)     streaming/                  │
│  ETL — transformar dados            glue/jobs/                  │
│  ELT — carregar e transformar       redshift/stored_procedures/ │
│  Data Warehouse + Star Schema       redshift/ddl/               │
│  Orquestração de pipelines          airflow/dags/               │
│  Qualidade de dados                 data_quality/               │
│  Governança e segurança             governance/                 │
│  Monitoramento                      monitoring/                 │
│  Lifecycle de dados (archiving)     s3/scripts/                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Arquitetura Geral

```
╔══════════════════════════════════════════════════════════════════════╗
║                    FONTES DE DADOS                                    ║
║                                                                       ║
║  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐        ║
║  │  CRM/ERP  │  │  REST API │  │  CSV/File │  │  Website  │        ║
║  │(clientes) │  │  (vendas) │  │ (catálogo)│  │ (eventos) │        ║
║  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘        ║
╚════════╪══════════════╪══════════════╪══════════════╪══════════════╝
         │              │              │              │
         │    INGESTÃO BATCH           │    INGESTÃO STREAMING
         │    (Airbyte)                │    (Kafka / Jitsu)
         ▼              ▼              ▼              ▼
╔══════════════════════════════════════════════════════════════════════╗
║                    S3 — DATA LAKE                                     ║
║                                                                       ║
║  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────┐    ║
║  │   raw/          │  │   processed/    │  │   archived/      │    ║
║  │                 │  │                 │  │                  │    ║
║  │  Dados brutos   │  │  Dados limpos   │  │  Dados antigos   │    ║
║  │  como chegam    │  │  em Parquet     │  │  comprimidos     │    ║
║  │  JSON / CSV     │  │  particionados  │  │  (S3 Glacier)    │    ║
║  └────────┬────────┘  └────────▲────────┘  └──────────────────┘    ║
╚═══════════╪════════════════════╪════════════════════════════════════╝
            │                    │
            │    TRANSFORMAÇÃO   │
            │    (Glue ETL)      │
            ▼                    │
╔═══════════════════════════════╗│
║  GLUE JOBS                    ╠╝
║                               ║         ╔══════════════════════╗
║  • Limpar dados               ║         ║  LAMBDA FUNCTIONS    ║
║  • Validar schema             ║◄────────║                      ║
║  • JSON/CSV → Parquet         ║         ║  • Validar schema    ║
║  • Agregar por dia/região     ║         ║  • Notificar falhas  ║
╚══════════════╦════════════════╝         ║  • Arquivar dados    ║
               │                          ╚══════════════════════╝
               ▼
╔══════════════════════════════════════════════════════════════════════╗
║                 REDSHIFT — DATA WAREHOUSE                             ║
║                                                                       ║
║  ┌──────────────┐    ┌──────────────────────┐    ┌───────────────┐  ║
║  │   staging    │───▶│   dw (Star Schema)   │───▶│  data marts   │  ║
║  │              │    │                      │    │               │  ║
║  │  Dados brutos│    │  dim_clientes        │    │  vm_vendas_   │  ║
║  │  vindos do   │    │  dim_produto         │    │  diarias      │  ║
║  │  S3          │    │  dim_tempo           │    │               │  ║
║  │              │    │  fato_vendas         │    │  vm_top_      │  ║
║  └──────────────┘    └──────────────────────┘    │  produtos     │  ║
║                                                    └───────────────┘  ║
╚══════════════════════════════════════════════════════════════════════╝
               ▲
╔══════════════╩═══════════════════════════════════════════════════════╗
║              AIRFLOW — ORQUESTRAÇÃO                                   ║
║                                                                       ║
║  DAG Clientes (diário) → DAG Vendas (horário) → DAG ETL → DAG Archive║
╚══════════════════════════════════════════════════════════════════════╝

    ╔════════════╗   ╔══════════════╗   ╔═══════════════╗
    ║    SQS     ║   ║ DATA QUALITY ║   ║  MONITORING   ║
    ║            ║   ║              ║   ║               ║
    ║ vendas_    ║   ║ Valida cada  ║   ║ Alertas de    ║
    ║ queue      ║   ║ etapa do     ║   ║ falha,        ║
    ║ dlq_errors ║   ║ pipeline     ║   ║ freshness,    ║
    ╚════════════╝   ╚══════════════╝   ╚═══════════════╝
```

---

## 3. Mapeamento Local → AWS

| Pasta / Ferramenta Local | Serviço AWS Real | Por que esse serviço? |
|---|---|---|
| `s3/raw/`, `s3/processed/`, `s3/archived/` | **Amazon S3** | Armazenamento barato e infinito para qualquer formato de arquivo |
| `glue/jobs/` | **AWS Glue** | Roda scripts ETL sem precisar gerenciar servidores |
| `glue/catalog/` | **AWS Glue Data Catalog** | Catálogo central de schemas e metadados |
| `glue/crawlers/` | **AWS Glue Crawlers** | Descobre automaticamente schemas de novos arquivos |
| `lambda/functions/` | **AWS Lambda** | Funções pequenas que executam em resposta a eventos |
| `lambda/triggers/` | **S3 Event Notifications + SQS Triggers** | Dispara Lambda quando arquivo chega ou mensagem entra na fila |
| `sqs/` | **Amazon SQS** | Fila de mensagens para desacoplar sistemas |
| `redshift/` (PostgreSQL local) | **Amazon Redshift** | Data Warehouse colunar para analytics em escala |
| `airflow/dags/` | **AWS MWAA** (Managed Airflow) | Agenda e orquestra todos os pipelines |
| `airbyte/` | **AWS DMS + Glue Connectors** | Conecta e sincroniza dados de fontes externas |
| `streaming/producers/` + `consumers/` | **Amazon Kinesis + Jitsu** | Processa eventos em tempo real (cliques, compras) |
| `data_quality/` | **AWS Deequ + Great Expectations** | Valida qualidade dos dados automaticamente |
| `governance/iam/` | **AWS IAM** | Controle de acesso (quem pode ler/escrever o quê) |
| `monitoring/` | **Amazon CloudWatch** | Métricas, logs e alertas da infraestrutura |

---

## 4. Fluxo de Dados Passo a Passo

### Batch (Diário / Horário)

```
PASSO 1 — INGESTÃO
  Airbyte conecta no CRM (PostgreSQL) e na API de vendas
  → Extrai os dados novos desde a última execução
  → Salva em s3/raw/clientes/*.json  e  s3/raw/vendas/*.csv
  → (Localmente: generate_sample_data.py faz isso)

PASSO 2 — DETECÇÃO
  S3 detecta novo arquivo chegando em raw/
  → Dispara evento → Lambda valida o schema
  → Se OK: publica mensagem na fila SQS vendas_queue
  → Se falhou: vai para dlq_errors (Dead Letter Queue)

PASSO 3 — TRANSFORMAÇÃO (ETL)
  Glue Job lê o arquivo bruto de s3/raw/
  → Limpa: remove duplicatas, trata nulos, padroniza datas
  → Valida: verifica tipos, faixas de valores esperados
  → Converte: JSON/CSV → Parquet (formato colunar, 10x mais rápido)
  → Salva em s3/processed/ com particionamento por data

PASSO 4 — CARGA (ELT)
  Redshift lê os arquivos Parquet de s3/processed/
  → Carrega na camada staging (dados brutos no banco)
  → Stored Procedure transforma staging → Star Schema (dw)
  → Views materializam os Data Marts para BI

PASSO 5 — QUALIDADE
  Great Expectations roda checks automáticos:
  → Clientes: CPF não nulo, email com @, cidade válida
  → Vendas: valor > 0, data de entrega > data de venda
  → Se falhar: alerta vai para monitoring/

PASSO 6 — ARCHIVING
  Lambda / Airflow verifica arquivos com mais de 90 dias em raw/
  → Move para s3/archived/ comprimido (.gz)
  → Na AWS: S3 Lifecycle Policy faz isso automaticamente
```

### Streaming (Tempo Real)

```
EVENTO acontece (usuário clica em produto no site)
  → Producer envia para tópico Kafka "eventos-clicks"
  → Consumer lê em tempo real
  → Salva em s3/raw/eventos/
  → Analytics em tempo real (contagem por produto, sessão)
```

---

## 5. Componentes Detalhados

### 5.1 S3 — Data Lake

**O que é:** Armazenamento de arquivos em "buckets" (baldes). Guarda qualquer formato, qualquer tamanho.

**Por que usar:** É o coração da arquitetura moderna de dados. Barato ($0,023/GB/mês na AWS), durável (99.999999999% de disponibilidade) e se integra com todos os outros serviços.

**Estrutura de particionamento:**
```
s3/raw/vendas/
    ano=2026/
        mes=04/
            vendas_20260412_165657.csv   ← arquivo do dia 12/04
            vendas_20260413_090000.csv   ← arquivo do dia 13/04
        mes=05/
            vendas_20260501_090000.csv
```
Particionamento por data permite que o Glue/Athena leia **só o mês que interessa**, sem escanear tudo.

**Lifecycle (ciclo de vida dos dados):**
```
raw/        → dados chegam aqui (acesso frequente)
processed/  → dados transformados (acesso moderado)
archived/   → dados com +90 dias (acesso raro, mais barato)
```

**Arquivos relevantes:**
- [s3/scripts/lifecycle_manager.py](s3/scripts/lifecycle_manager.py) — move arquivos antigos para archived/
- [s3/scripts/s3_utils.py](s3/scripts/s3_utils.py) — funções utilitárias (listar, mover, deletar)

---

### 5.2 Glue — ETL

**O que é:** Serviço que roda scripts Python/PySpark para transformar dados, sem precisar de servidor.

**Por que usar:** Na AWS, o Glue escala automaticamente. Você paga só pelo tempo de execução. Aqui simulamos com Python puro.

**Jobs disponíveis:**

| Job | Entrada | Saída | O que faz |
|---|---|---|---|
| [clientes_raw_to_processed.py](glue/jobs/clientes_raw_to_processed.py) | `s3/raw/clientes/*.json` | `s3/processed/clientes/*.parquet` | Limpa, valida CPF, padroniza cidades |
| [vendas_raw_to_processed.py](glue/jobs/vendas_raw_to_processed.py) | `s3/raw/vendas/*.csv` | `s3/processed/vendas/*.parquet` | Limpa, valida valores, join com clientes |
| [daily_aggregations.py](glue/jobs/daily_aggregations.py) | `s3/processed/vendas/` | tabelas agregadas | Totaliza vendas por dia, categoria, UF |

**Data Catalog:** Registra o schema de cada tabela (nome das colunas, tipos, localização).
- [glue/catalog/schema_clientes.json](glue/catalog/schema_clientes.json)
- [glue/catalog/schema_vendas.json](glue/catalog/schema_vendas.json)

**Crawlers:** Escaneia s3/raw/ e descobre automaticamente novos schemas.
- [glue/crawlers/crawler_simulator.py](glue/crawlers/crawler_simulator.py)

---

### 5.3 Lambda — Funções event-driven

**O que é:** Funções pequenas que executam em resposta a eventos. Sem servidor, cobra por execução.

**Por que usar:** Ideal para tarefas pontuais disparadas por eventos (arquivo chegou, mensagem na fila, horário agendado).

**Funções disponíveis:**

| Função | Trigger | O que faz |
|---|---|---|
| [validate_schema.py](lambda/functions/validate_schema.py) | Novo arquivo em s3/raw/ | Verifica se o arquivo tem as colunas esperadas |
| [notify_pipeline.py](lambda/functions/notify_pipeline.py) | Falha no pipeline | Envia alerta (email/Slack) |
| [archive_old_data.py](lambda/functions/archive_old_data.py) | Agendado (diário) | Move arquivos +90 dias para archived/ |

**Triggers:**
- [s3_event_trigger.py](lambda/triggers/s3_event_trigger.py) — simula evento S3 → Lambda
- [sqs_consumer.py](lambda/triggers/sqs_consumer.py) — lê mensagens da fila SQS e processa

---

### 5.4 SQS — Filas de Mensagens

**O que é:** Fila de mensagens para comunicação assíncrona entre sistemas.

**Por que usar:** Desacopla produtor do consumidor. Se o consumer cair, as mensagens ficam na fila esperando. Evita perda de dados.

**Analogia:** É como uma caixa de entrada de email. Quem envia não precisa esperar o destinatário ler. Quando o destinatário estiver pronto, lê e processa.

**Filas configuradas:**

| Fila | Propósito |
|---|---|
| `vendas_queue` | Mensagens de novas vendas aguardando processamento |
| `dlq_errors` | Dead Letter Queue — mensagens que falharam 3 vezes |

**Conceito importante — Dead Letter Queue (DLQ):**
```
Mensagem entra na fila → Consumer tenta processar → FALHA
                                                        ↓
                                               Tentativa 2 → FALHA
                                                        ↓
                                               Tentativa 3 → FALHA
                                                        ↓
                                    Vai para dlq_errors (para análise)
```

**Arquivos relevantes:**
- [sqs/sqs_local.py](sqs/sqs_local.py) — simulador local de SQS
- [sqs/queues/vendas_queue.json](sqs/queues/vendas_queue.json) — configuração da fila

---

### 5.5 Redshift — Data Warehouse

**O que é:** Banco de dados colunar otimizado para consultas analíticas em grandes volumes.

**Por que usar:** Diferente do PostgreSQL comum (que guarda dados por linha), o Redshift guarda por coluna — muito mais rápido para `SELECT SUM(valor) FROM vendas WHERE ano=2025`.

**Simulação local:** Usamos PostgreSQL (SQL compatível com Redshift).

**Camadas do Data Warehouse:**

```
staging (dados brutos vindos do S3)
    ↓  stored procedure sp_load_staging.sql
dw — Star Schema (dados modelados)
    ↓  stored procedure sp_transform_dw.sql
dm — Data Marts (views agregadas para BI)
```

**Star Schema (Esquema Estrela):**
```
              dim_tempo
                 │
dim_produto ─── fato_vendas ─── dim_clientes
                 │
              dim_canal
```

A tabela `fato_vendas` fica no centro com os números (valor, quantidade, custo).
As `dim_*` ficam em volta com os detalhes (quem comprou, o quê, quando, onde).

**Arquivos SQL:**
- [redshift/ddl/001_create_schemas.sql](redshift/ddl/001_create_schemas.sql) — cria schemas: staging, dw, dm
- [redshift/ddl/002_staging_tables.sql](redshift/ddl/002_staging_tables.sql) — tabelas de staging
- [redshift/ddl/003_dw_tables.sql](redshift/ddl/003_dw_tables.sql) — Star Schema completo
- [redshift/ddl/004_data_marts.sql](redshift/ddl/004_data_marts.sql) — views para BI
- [redshift/stored_procedures/sp_load_staging.sql](redshift/stored_procedures/sp_load_staging.sql) — carga staging
- [redshift/stored_procedures/sp_transform_dw.sql](redshift/stored_procedures/sp_transform_dw.sql) — transformação dw

---

### 5.6 Airflow — Orquestração

**O que é:** Ferramenta que agenda, monitora e gerencia pipelines de dados como um grafo de tarefas (DAG).

**Por que usar:** Sem orquestração, você rodaria scripts manualmente. O Airflow garante ordem, dependências, retentativas e histórico de execução.

**DAG = Directed Acyclic Graph (Grafo Acíclico Dirigido):** Define quais tarefas rodam, em que ordem e quando.

**DAGs disponíveis:**

| DAG | Frequência | O que faz |
|---|---|---|
| [dag_ingestion_clientes.py](airflow/dags/dag_ingestion_clientes.py) | Diário (02:00) | Ingere clientes do CRM → s3/raw/ |
| [dag_ingestion_vendas.py](airflow/dags/dag_ingestion_vendas.py) | Horário | Ingere vendas da API → s3/raw/ |
| [dag_etl_full_pipeline.py](airflow/dags/dag_etl_full_pipeline.py) | Diário (04:00) | Raw → Processed → Redshift |
| [dag_archiving.py](airflow/dags/dag_archiving.py) | Semanal (domingo) | Move dados antigos para archived/ |

**Exemplo de dependência no DAG completo:**
```
validar_schema → glue_clientes → glue_vendas → load_staging → transform_dw → data_quality → notificar_sucesso
```
Se `glue_clientes` falhar, os passos seguintes não executam.

---

### 5.7 Airbyte — Ingestão de fontes externas

**O que é:** Ferramenta open-source que conecta em centenas de fontes (bancos, APIs, SaaS) e sincroniza dados automaticamente.

**Por que usar:** Sem o Airbyte, você escreveria um script de extração para cada fonte. Com o Airbyte, você configura via YAML/UI e ele cuida da extração, paginação, retry, etc.

**Fontes configuradas:**
- [airbyte/sources/source_postgres_crm.yaml](airbyte/sources/source_postgres_crm.yaml) — CRM em PostgreSQL
- [airbyte/sources/source_api_vendas.yaml](airbyte/sources/source_api_vendas.yaml) — API REST de vendas
- [airbyte/sources/source_csv_catalog.yaml](airbyte/sources/source_csv_catalog.yaml) — Catálogo em CSV

**Destino:** Todos escrevem em `s3/raw/` (simulado localmente pelo `generate_sample_data.py`).

**Modos de sync:**
- `full_refresh` — extrai tudo toda vez (clientes, catálogo)
- `incremental` — extrai só o que mudou desde a última execução (vendas — mais eficiente)

---

### 5.8 Streaming — Kafka / Jitsu

**O que é:** Processamento de eventos em tempo real. Diferente do batch (que roda de hora em hora), o streaming processa cada evento no momento que acontece.

**Por que usar:** Para casos onde latência importa — dashboard em tempo real, detecção de fraude, recomendação de produto durante a sessão.

**Kafka local = Amazon Kinesis na AWS.**

**Tópicos (equivalente a "tabelas" no Kafka):**
- `eventos-clicks` — cliques de usuários no site
- `eventos-compras` — compras realizadas

**Produtores (quem gera eventos):**
- [streaming/producers/evento_click_producer.py](streaming/producers/evento_click_producer.py)
- [streaming/producers/evento_compra_producer.py](streaming/producers/evento_compra_producer.py)

**Consumidores (quem processa):**
- [streaming/consumers/consumer_eventos.py](streaming/consumers/consumer_eventos.py) — salva em s3/raw/eventos/
- [streaming/consumers/consumer_analytics.py](streaming/consumers/consumer_analytics.py) — agregações em tempo real

---

### 5.9 Data Quality

**O que é:** Validações automáticas que garantem que os dados estão corretos antes de chegar no Data Warehouse.

**Por que usar:** Dados errados no DW = relatórios errados = decisões erradas. Melhor detectar cedo.

**Checks configurados para Clientes:**
- CPF não pode ser nulo
- CPF deve ter 11 dígitos
- Email deve conter @
- Cidade deve ser não vazia
- Limite de crédito deve ser > 0

**Checks configurados para Vendas:**
- Valor total deve ser > 0
- Data de entrega deve ser posterior à data de venda
- Status deve ser um dos valores esperados (CONCLUIDA, CANCELADA, etc.)
- Produto ID não pode ser nulo

**Arquivos:**
- [data_quality/checks/dq_clientes.py](data_quality/checks/dq_clientes.py)
- [data_quality/checks/dq_vendas.py](data_quality/checks/dq_vendas.py)
- [data_quality/expectations/clientes_expectations.json](data_quality/expectations/clientes_expectations.json)

---

### 5.10 Governance — Governança

**O que é:** Controle de quem pode acessar o quê, e documentação de todos os dados.

**Por que usar:** Segurança, compliance (LGPD), e para que o time de negócio saiba o que cada dado significa.

**IAM (Identity and Access Management):**
```
glue-role       → pode ler s3/raw/ e escrever s3/processed/
lambda-role     → pode ler/escrever s3/ e publicar em SQS
airflow-role    → pode disparar Glue jobs e Lambda
redshift-role   → pode ler s3/processed/ para carga
bi-role         → pode apenas SELECT em dm (data marts)
```

**Data Dictionary:** Documenta cada tabela e coluna do Data Warehouse.
- [governance/catalog/data_dictionary.md](governance/catalog/data_dictionary.md)

---

### 5.11 Monitoring — Monitoramento

**O que é:** Observabilidade da infraestrutura — métricas, alertas e dashboards.

**Por que usar:** Saber quando algo quebrou antes que o usuário perceba.

**Alertas configurados:**
- Pipeline não executou nas últimas 25 horas
- Fila SQS acumulando mais de 1.000 mensagens
- Contagem de linhas caiu mais de 20% em relação ao dia anterior
- Erro em job do Glue

**Arquivos:**
- [monitoring/alerts/alert_rules.json](monitoring/alerts/alert_rules.json)
- [monitoring/dashboards/pipeline_health.json](monitoring/dashboards/pipeline_health.json)

---

## 6. Estrutura de Pastas

```
Data_Platform_ETL_ELT_Streaming/
│
├── s3/                          # Simula Amazon S3 (Data Lake)
│   ├── raw/                     # Dados brutos como chegam das fontes
│   │   ├── clientes/            # JSON do CRM — particionado por ano/mes/dia
│   │   ├── vendas/              # CSV do ERP — particionado por ano/mes
│   │   └── eventos/             # JSON do Kafka — eventos do site
│   ├── processed/               # Dados limpos em formato Parquet
│   │   ├── clientes/
│   │   └── vendas/
│   ├── archived/                # Dados antigos comprimidos (simula S3 Glacier)
│   └── scripts/
│       ├── lifecycle_manager.py # Move dados para archived/ após 90 dias
│       └── s3_utils.py          # Utilitários: listar, mover, deletar arquivos
│
├── glue/                        # Simula AWS Glue (ETL serverless)
│   ├── jobs/
│   │   ├── clientes_raw_to_processed.py
│   │   ├── vendas_raw_to_processed.py
│   │   └── daily_aggregations.py
│   ├── catalog/                 # Schemas registrados (Glue Data Catalog)
│   │   ├── schema_clientes.json
│   │   └── schema_vendas.json
│   └── crawlers/
│       └── crawler_simulator.py # Descobre schemas de novos arquivos
│
├── lambda/                      # Simula AWS Lambda (funções event-driven)
│   ├── functions/
│   │   ├── validate_schema.py   # Valida schema ao chegar novo arquivo
│   │   ├── notify_pipeline.py   # Envia alertas de falha
│   │   └── archive_old_data.py  # Arquiva dados antigos
│   └── triggers/
│       ├── s3_event_trigger.py  # Disparo por evento S3
│       └── sqs_consumer.py      # Disparo por mensagem na fila
│
├── sqs/                         # Simula Amazon SQS (filas)
│   ├── queues/
│   │   ├── vendas_queue.json    # Config da fila principal
│   │   └── dlq_errors.json      # Dead Letter Queue (mensagens com erro)
│   └── sqs_local.py             # Simulador local de filas
│
├── redshift/                    # Simula Amazon Redshift (Data Warehouse)
│   ├── ddl/
│   │   ├── 001_create_schemas.sql   # Cria schemas: staging, dw, dm
│   │   ├── 002_staging_tables.sql   # Tabelas de staging
│   │   ├── 003_dw_tables.sql        # Star Schema: dim_* + fato_vendas
│   │   └── 004_data_marts.sql       # Views para BI
│   ├── stored_procedures/
│   │   ├── sp_load_staging.sql      # Carga S3 → staging
│   │   └── sp_transform_dw.sql      # staging → Star Schema
│   └── init_db.py                   # Executa todos os DDLs no PostgreSQL
│
├── airflow/                     # Orquestração de pipelines
│   ├── dags/
│   │   ├── dag_ingestion_clientes.py
│   │   ├── dag_ingestion_vendas.py
│   │   ├── dag_etl_full_pipeline.py
│   │   └── dag_archiving.py
│   ├── plugins/
│   │   └── custom_operators.py  # Operators customizados para S3/Glue local
│   └── config/
│       └── airflow.cfg
│
├── airbyte/                     # Configs de ingestão de fontes externas
│   ├── sources/                 # De onde vêm os dados
│   ├── destinations/            # Para onde vão
│   └── connections/             # Como sincronizar (modo, frequência)
│
├── streaming/                   # Kafka local (simula Kinesis + Jitsu)
│   ├── producers/               # Geram eventos (cliques, compras)
│   ├── consumers/               # Processam eventos em tempo real
│   └── jitsu_config/            # Config de coleta de eventos do site
│
├── data_quality/                # Qualidade de dados
│   ├── expectations/            # Regras de validação em JSON
│   └── checks/                  # Scripts que executam as validações
│
├── governance/                  # Governança e segurança
│   ├── iam/                     # Roles e políticas de acesso
│   └── catalog/
│       └── data_dictionary.md   # Documentação de todas as tabelas
│
├── monitoring/                  # Monitoramento e alertas
│   ├── alerts/                  # Regras de alerta
│   └── dashboards/              # Config de dashboards
│
├── scripts/                     # Scripts utilitários
│   ├── generate_sample_data.py  # Gera dados fictícios brasileiros
│   ├── setup.sh                 # Setup completo em um comando
│   └── run_full_pipeline.sh     # Roda o pipeline completo
│
├── docker-compose.yml           # Sobe PostgreSQL, Kafka, Airflow, pgAdmin
├── requirements.txt             # Dependências Python
├── .env.example                 # Template de variáveis de ambiente
└── DOCUMENTACAO.md              # Este arquivo
```

---

## 7. Como Rodar o Projeto

### Sem Docker (apenas Python — já testado e funcionando)

```bash
# Passo 1 — Gerar dados de exemplo (popula s3/raw/)
python3 scripts/generate_sample_data.py --clientes 200 --vendas 1000

# Passo 2 — Rodar ETL Glue (raw/ → processed/)
python3 glue/jobs/clientes_raw_to_processed.py
python3 glue/jobs/vendas_raw_to_processed.py

# Passo 3 — Rodar crawler (descobre schemas)
python3 glue/crawlers/crawler_simulator.py
```

### Com Docker (ambiente completo)

> Requer Docker Desktop com integração WSL ativada.

```bash
# Subir serviços base
docker-compose up -d postgres pgadmin kafka kafka-ui

# Inicializar banco de dados
python3 redshift/init_db.py

# Subir Airflow
docker-compose up -d airflow-webserver airflow-scheduler

# Acessar interfaces:
# pgAdmin (banco):  http://localhost:5050
# Airflow (DAGs):   http://localhost:8080  (admin / admin123)
# Kafka UI:         http://localhost:8090
```

---

## 8. Dados Gerados

O script `generate_sample_data.py` cria dados 100% brasileiros e fictícios:

**Clientes** (formato JSON):
```json
{
  "id": 1,
  "cpf": "50159279836",
  "nome": "Claudia Soares",
  "email": "claudia.soares371@uol.com.br",
  "cidade": "Ananindeua",
  "uf": "PA",
  "segmento_cliente": "B2C",
  "limite_credito": 5000.00,
  "ativo": true
}
```

**Vendas** (formato CSV):
```
id_venda, id_pedido,           produto_nome,      valor_total, canal_venda,  forma_pagamento, status_venda
1,        PED-20260109-000001, iPad Air 5ª Geração, 7485.74,  LOJA_FISICA,  CARTAO_DEBITO,   CONCLUIDA
2,        PED-20250817-000002, Fone JBL Tune 760NC, 468.40,   LOJA_FISICA,  CARTAO_DEBITO,   CONCLUIDA
```

**Produtos simulados:** Eletrônicos, Calçados, Vestuário, Casa, Esportes, Livros, Beleza, Alimentação

**Formas de pagamento:** PIX, Cartão de Crédito (parcelado), Cartão de Débito, Boleto

**Canais:** E-commerce, Loja Física, App Mobile, Televendas

---

## 9. Glossário

| Termo | Significado |
|---|---|
| **ETL** | Extract, Transform, Load — extrai de uma fonte, transforma, carrega no destino |
| **ELT** | Extract, Load, Transform — carrega primeiro, transforma depois (dentro do DW) |
| **Data Lake** | Repositório de dados brutos em qualquer formato (S3) |
| **Data Warehouse** | Banco otimizado para consultas analíticas (Redshift) |
| **Data Mart** | Subconjunto do DW focado em uma área de negócio (vendas, clientes) |
| **Star Schema** | Modelo com tabela fato no centro e dimensões ao redor |
| **Fato** | Tabela com métricas numéricas (valor de venda, quantidade) |
| **Dimensão** | Tabela com atributos descritivos (nome do cliente, nome do produto) |
| **Parquet** | Formato colunar de arquivo — muito mais eficiente que CSV para analytics |
| **Particionamento** | Organizar arquivos em pastas por data para leitura seletiva eficiente |
| **DAG** | Directed Acyclic Graph — grafo de tarefas do Airflow |
| **Crawler** | Programa que escaneia arquivos e descobre schemas automaticamente |
| **DLQ** | Dead Letter Queue — fila que recebe mensagens que falharam N vezes |
| **Lifecycle** | Política de ciclo de vida de dados (mover, comprimir, deletar por tempo) |
| **IAM** | Identity and Access Management — controle de acesso na AWS |
| **Streaming** | Processamento de dados em tempo real, evento a evento |
| **Batch** | Processamento em lote, executado em intervalos (hora, dia) |
| **Schema** | Estrutura dos dados: quais colunas existem e seus tipos |
| **Staging** | Área temporária onde dados brutos chegam antes de serem transformados |
| **LGPD** | Lei Geral de Proteção de Dados — lei brasileira de privacidade |

---

*Documentação gerada em 2026-04-12 | Projeto: Data Platform ETL/ELT/Streaming*
