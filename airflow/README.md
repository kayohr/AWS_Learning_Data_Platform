# Airflow - Orquestração de Pipelines (Simula AWS MWAA)

## O que é o Apache Airflow?

**Apache Airflow** é uma plataforma para criar, agendar e monitorar pipelines de dados.
Na AWS, é oferecido como serviço gerenciado: **MWAA (Managed Workflows for Apache Airflow)**.

### Conceitos Fundamentais

#### DAG (Directed Acyclic Graph)
Um DAG é o "fluxograma" do seu pipeline:
- **Directed**: As tarefas têm direção (A → B → C)
- **Acyclic**: Sem ciclos (A não pode depender de C se C depende de A)
- **Graph**: Conjunto de nós (tasks) e arestas (dependências)

```python
# Exemplo de DAG simples
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG('meu_pipeline', start_date=datetime(2024, 1, 1), schedule='@daily') as dag:
    tarefa_1 = PythonOperator(task_id='extrair', python_callable=extrair)
    tarefa_2 = PythonOperator(task_id='transformar', python_callable=transformar)
    tarefa_3 = PythonOperator(task_id='carregar', python_callable=carregar)

    tarefa_1 >> tarefa_2 >> tarefa_3  # Define ordem de execução
```

#### Operators (Operadores)
Templates reutilizáveis para tipos comuns de tarefas:

| Operator              | Para quê                                |
|-----------------------|-----------------------------------------|
| PythonOperator        | Executar função Python                  |
| PostgresOperator      | Executar SQL no PostgreSQL/Redshift     |
| BashOperator          | Executar comando shell                  |
| S3ToRedshiftOperator  | COPY do S3 para Redshift                |
| GlueJobOperator       | Disparar job AWS Glue                   |
| LambdaInvokeFunctionOperator | Invocar AWS Lambda               |
| EmailOperator         | Enviar e-mail                           |
| SlackAPIPostOperator  | Postar no Slack                         |

#### Schedule (Agendamento)
Expressões cron para agendar execuções:

```
@hourly    = toda hora          = 0 * * * *
@daily     = todo dia meia-noite = 0 0 * * *
@weekly    = todo domingo       = 0 0 * * 0
0 6 * * *  = todo dia às 06:00
0 8 * * 1-5 = dias úteis às 08:00
```

## DAGs deste Projeto

### 1. dag_ingestion_clientes.py
- **Schedule**: Diário às 02:00
- **Pipeline**: Airbyte coleta do CRM → S3 raw/ → Lambda valida
- **Dependência**: Necessário antes de dag_etl_full_pipeline

### 2. dag_ingestion_vendas.py
- **Schedule**: A cada hora (vendas acontecem o dia todo)
- **Pipeline**: API vendas → S3 raw/ → Lambda valida
- **Tipo**: Incremental (só processa novas vendas)

### 3. dag_etl_full_pipeline.py
- **Schedule**: Diário às 04:00 (depois das ingestões)
- **Pipeline**: S3 raw/ → Glue ETL → S3 processed/ → Redshift staging → DW
- **Dependência**: dag_ingestion_clientes e dag_ingestion_vendas devem ter rodado

### 4. dag_archiving.py
- **Schedule**: Semanal (domingo às 01:00)
- **Pipeline**: Verifica arquivos antigos → Lifecycle Manager → Archive/Delete

## XCom (Cross-Communication)
Permite que tarefas passem dados entre si:

```python
# Tarefa 1: empurra dados
def tarefa_extrair(**context):
    qtd_registros = 5000
    context['ti'].xcom_push(key='qtd_registros', value=qtd_registros)

# Tarefa 2: puxa dados da tarefa anterior
def tarefa_transformar(**context):
    qtd = context['ti'].xcom_pull(task_ids='extrair', key='qtd_registros')
    print(f"Vou transformar {qtd} registros")
```

## Monitoramento no Airflow UI

Acesse: http://localhost:8080 (admin/admin123)

- **Graph View**: Visualiza o DAG como grafo
- **Gantt View**: Timeline de execução das tarefas
- **Tree View**: Histórico de execuções
- **Task Logs**: Logs detalhados de cada task

## Callbacks (Notificações)

```python
# Notifica quando uma task falha
def on_failure_callback(context):
    from lambda.functions.notify_pipeline import notificar_falha_pipeline
    notificar_falha_pipeline(
        pipeline_nome=context['dag'].dag_id,
        mensagem_erro=str(context.get('exception', 'Erro desconhecido'))
    )

with DAG('meu_dag', on_failure_callback=on_failure_callback) as dag:
    ...
```
