# Plataforma de Dados Local - Simulação AWS

## Visão Geral

Este projeto simula uma **Plataforma de Dados completa na AWS** usando ferramentas locais.
É um ambiente de aprendizado onde cada pasta representa um serviço AWS real.

## Por que simular localmente?

Aprender AWS diretamente pode ser caro e complexo. Aqui você aprende os **conceitos**
com ferramentas gratuitas e locais, depois migra facilmente para a nuvem.

## Diagrama de Arquitetura

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                     PLATAFORMA DE DADOS - ARQUITETURA COMPLETA                  ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                  ║
║  FONTES DE DADOS (Sources)                                                       ║
║  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐           ║
║  │   CRM/ERP   │  │  REST APIs  │  │  Arquivos   │  │   Website   │           ║
║  │ (PostgreSQL)│  │  (Vendas)   │  │    CSV      │  │  (Eventos)  │           ║
║  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘           ║
║         │                │                 │                 │                   ║
║  ╔══════▼════════════════▼═════════════════▼══╗    ╔════════▼════════╗          ║
║  ║         AIRBYTE (Ingestão Batch)            ║    ║ JITSU / KAFKA   ║          ║
║  ║    AWS Real: DMS + Glue Connectors          ║    ║ AWS Real:Kinesis║          ║
║  ╚══════════════════════╦════════════════════╝    ╚════════╦════════╝          ║
║                          │                                   │                   ║
║  ╔═══════════════════════▼═══════════════════════════════════▼════════╗          ║
║  ║                    S3 - LAGO DE DADOS (Data Lake)                   ║          ║
║  ║                         AWS Real: Amazon S3                         ║          ║
║  ║  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             ║          ║
║  ║  │  raw/        │  │  processed/  │  │  archived/   │             ║          ║
║  ║  │ (dados brutos│  │ (Parquet,    │  │ (dados frios,│             ║          ║
║  ║  │  como chegam)│  │  limpos)     │  │  Glacier)    │             ║          ║
║  ║  └──────┬───────┘  └──────▲───────┘  └──────▲───────┘             ║          ║
║  ╚═════════╪═════════════════╪═════════════════╪═════════════════════╝          ║
║            │                 │                 │                                  ║
║  ╔═════════▼═════════════════╪═╗    ╔══════════╪══════════════╗                 ║
║  ║    GLUE - Jobs ETL         ║    ║   LAMBDA - Funções        ║                 ║
║  ║  AWS Real: AWS Glue        ║    ║   AWS Real: AWS Lambda    ║                 ║
║  ║  • raw → processed         ║    ║   • Validação de schema   ║                 ║
║  ║  • JSON/CSV → Parquet      ╠════╣   • Notificações          ║                 ║
║  ║  • Aggregations            ║    ║   • Arquivamento auto     ║                 ║
║  ╚═════════╦══════════════════╝    ╚═══════════════════════════╝                 ║
║            │                                                                      ║
║  ╔═════════▼══════════════════════════════════════════════════╗                  ║
║  ║              REDSHIFT - Data Warehouse                      ║                  ║
║  ║           AWS Real: Amazon Redshift                         ║                  ║
║  ║  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        ║                  ║
║  ║  │  staging    │  │  dw (Star   │  │  dm (Data   │        ║                  ║
║  ║  │  (dados     │  │  Schema)    │  │  Marts/     │        ║                  ║
║  ║  │  brutos)    │  │  dim+fato   │  │  Relatórios)│        ║                  ║
║  ║  └─────────────┘  └─────────────┘  └─────────────┘        ║                  ║
║  ╚═════════════════════════════════════════════════════════════╝                  ║
║                                                                                   ║
║  ╔══════════════════════════════════════════════════════════╗                    ║
║  ║           AIRFLOW - Orquestração (Scheduling)            ║                    ║
║  ║              AWS Real: AWS MWAA (Managed Airflow)        ║                    ║
║  ║  DAG Clientes → DAG Vendas → DAG ETL → DAG Archiving    ║                    ║
║  ╚══════════════════════════════════════════════════════════╝                    ║
║                                                                                   ║
║  ╔════════════════╗  ╔════════════════╗  ╔══════════════════╗                   ║
║  ║ SQS - Filas    ║  ║ DATA QUALITY   ║  ║  MONITORING      ║                   ║
║  ║ AWS Real: SQS  ║  ║ Great Expect.  ║  ║  CloudWatch/     ║                   ║
║  ║ • vendas_queue ║  ║ • clientes     ║  ║  Grafana local   ║                   ║
║  ║ • dlq_errors   ║  ║ • vendas       ║  ║  • alertas       ║                   ║
║  ╚════════════════╝  ╚════════════════╝  ╚══════════════════╝                   ║
╚══════════════════════════════════════════════════════════════════════════════════╝
```

## Mapeamento: Local → AWS Real

| Pasta Local         | Serviço AWS Real       | Função                                    |
|---------------------|------------------------|-------------------------------------------|
| `s3/`               | Amazon S3              | Armazenamento de objetos (Data Lake)      |
| `glue/`             | AWS Glue               | ETL serverless + catálogo de dados        |
| `lambda/`           | AWS Lambda             | Funções serverless event-driven           |
| `sqs/`              | Amazon SQS             | Filas de mensagens assíncronas            |
| `redshift/`         | Amazon Redshift        | Data Warehouse colunar                    |
| `airflow/`          | AWS MWAA               | Orquestração de pipelines                 |
| `airbyte/`          | AWS DMS + Glue         | Ingestão de dados de várias fontes        |
| `streaming/`        | Amazon Kinesis         | Streaming de eventos em tempo real        |
| PostgreSQL local    | Amazon Redshift        | Banco de dados (simulação local)          |
| Kafka local         | Amazon Kinesis         | Streaming de mensagens                    |

## Como Começar

### Pré-requisitos
- Docker Desktop instalado
- Python 3.9+
- 8GB RAM disponível

### Setup em um comando
```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

### Ou passo a passo:
```bash
# 1. Copiar variáveis de ambiente
cp .env.example .env

# 2. Instalar dependências Python
pip install -r requirements.txt

# 3. Subir todos os serviços Docker
docker-compose up -d

# 4. Inicializar banco de dados (Redshift simulado)
python redshift/init_db.py

# 5. Gerar dados de exemplo
python scripts/generate_sample_data.py

# 6. Rodar pipeline completo
./scripts/run_full_pipeline.sh
```

## Fluxo de Dados

```
1. INGESTÃO (Airbyte/Jitsu)
   CRM → s3/raw/clientes/
   API  → s3/raw/vendas/
   Web  → s3/raw/eventos/ (via Kafka)

2. TRANSFORMAÇÃO (Glue ETL)
   s3/raw/ → Limpar → Validar → s3/processed/ (Parquet)

3. CARGA (Redshift)
   s3/processed/ → staging → dw (star schema) → data marts

4. ORQUESTRAÇÃO (Airflow)
   Agenda e monitora todos os passos acima

5. QUALIDADE (Great Expectations)
   Valida dados em cada etapa
```

## Conceitos de AWS que Você Vai Aprender

1. **S3**: Buckets, prefixes, lifecycle policies, storage classes (Standard → Glacier)
2. **Glue**: Data Catalog, Crawlers, ETL Jobs com PySpark
3. **Lambda**: Event-driven computing, triggers, cold start
4. **SQS**: Message queues, visibility timeout, dead-letter queues
5. **Redshift**: Columnar storage, distribution keys, sort keys, VACUUM, ANALYZE
6. **Airflow (MWAA)**: DAGs, operators, XCom, task dependencies
7. **Kinesis/Kafka**: Partitions, consumer groups, offset management
8. **IAM**: Roles, policies, least privilege principle

## Estrutura de Custos AWS (para referência)

| Serviço    | Custo Aprox. AWS      | Local (aqui)    |
|------------|-----------------------|-----------------|
| S3         | $0.023/GB/mês         | Grátis (disco)  |
| Glue       | $0.44/DPU-hora        | Grátis (Python) |
| Lambda     | $0.20/1M requisições  | Grátis (Python) |
| SQS        | $0.40/1M mensagens    | Grátis (Python) |
| Redshift   | ~$0.25/hora/nó        | Grátis (PG)     |
| MWAA       | ~$0.49/hora           | Grátis (Docker) |

---
*Projeto educacional - Simulação local de arquitetura AWS para aprendizado*
