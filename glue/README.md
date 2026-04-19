# Glue - Simulação do AWS Glue (ETL Serverless)

## O que é o AWS Glue?

**AWS Glue** é o serviço de ETL (Extract, Transform, Load) serverless da AWS.
"Serverless" significa que você não precisa gerenciar servidores - a AWS cuida de tudo.

### Componentes Principais do AWS Glue

#### 1. Data Catalog (Catálogo de Dados)
O Glue mantém um catálogo central com:
- **Databases**: Grupos de tabelas relacionadas
- **Tables**: Definição do schema (colunas, tipos, localização no S3)
- **Partitions**: Metadados sobre partições (ex: data=2024-01-15)

```
# É como um "biblioteca" que sabe onde estão todos os dados
# No AWS: acessado via Athena, Redshift Spectrum, EMR
# Local: simulado com arquivos JSON em catalog/
```

#### 2. Crawlers (Rastreadores)
Programas que **descobrem automaticamente** o schema dos dados no S3:
```
S3: s3://bucket/raw/clientes/2024-01-15/clientes.json
    ↓ Glue Crawler analisa o arquivo
    ↓ Detecta colunas: id, nome, cpf, email, cidade
    ↓ Atualiza o Data Catalog
Resultado: Tabela "clientes" criada no Catalog
```

#### 3. ETL Jobs (Jobs de Transformação)
Scripts Python ou Scala que transformam dados:
```
RAW (JSON/CSV) → Glue Job → PROCESSED (Parquet, limpo, validado)
```

Os Jobs rodam em **Apache Spark** na AWS (DPUs - Data Processing Units).
Localmente, usamos **Pandas** (mais simples, mesma lógica).

## Arquitetura ETL no Glue

```
S3 Raw          Glue Job              S3 Processed
─────────       ─────────────────     ────────────
clientes.json → lê JSON             → clientes.parquet
                valida schema
                limpa dados
                deduplica
                converte Parquet

vendas.csv    → lê CSV              → vendas.parquet
                join com clientes
                calcula totais
                converte Parquet
```

## Diferenças: Glue Real vs Simulação Local

| Aspecto           | AWS Glue Real                | Simulação Local          |
|-------------------|------------------------------|--------------------------|
| Motor             | Apache Spark (distribuído)   | Pandas (single machine)  |
| Escala            | PB de dados, cluster auto    | Limitado pela RAM        |
| Custo             | $0.44/DPU-hora               | Grátis                   |
| Data Catalog      | Totalmente gerenciado        | Arquivos JSON            |
| Schema Evolution  | Automático                   | Manual                   |
| Scheduling        | Via console ou Airflow       | Via Airflow local        |
| Monitoramento     | CloudWatch nativo            | Logs no terminal         |

## Como Funciona o DPU (Data Processing Unit)?

No Glue real:
- 1 DPU = 4 vCPUs + 16GB RAM
- Job ETL típico: 10 DPUs = 40 vCPUs + 160GB RAM
- Preço: $0.44/DPU-hora
- Job de 10 min com 10 DPUs = ~$0.73

Localmente, rodamos com a RAM do seu computador.

## Padrão de Transformação

Os jobs seguem a ordem:
```
1. EXTRACT   → Lê dados do S3 raw/
2. VALIDATE  → Verifica schema e qualidade
3. CLEAN     → Remove duplicatas, valores nulos, formatos errados
4. TRANSFORM → Aplica regras de negócio, joins, cálculos
5. LOAD      → Salva em S3 processed/ no formato Parquet
```

## Executando os Jobs Localmente

```bash
# Job de clientes (JSON → Parquet)
python glue/jobs/clientes_raw_to_processed.py

# Job de vendas (CSV → Parquet + join)
python glue/jobs/vendas_raw_to_processed.py

# Job de agregações diárias
python glue/jobs/daily_aggregations.py

# Crawler (descobre schemas automaticamente)
python glue/crawlers/crawler_simulator.py
```

## Data Catalog - Schema dos Dados

Os schemas ficam em `glue/catalog/` como arquivos JSON.
Isso simula o Glue Data Catalog da AWS.

No AWS real, você acessaria o catálogo via:
```python
import boto3
glue = boto3.client('glue')

# Criar tabela no catálogo
glue.create_table(
    DatabaseName='raw_db',
    TableInput={
        'Name': 'clientes',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'id', 'Type': 'bigint'},
                {'Name': 'nome', 'Type': 'string'},
                {'Name': 'cpf', 'Type': 'string'},
            ],
            'Location': 's3://bucket/raw/clientes/',
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe'}
        }
    }
)
```
