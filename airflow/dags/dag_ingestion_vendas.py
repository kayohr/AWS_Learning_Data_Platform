"""
dag_ingestion_vendas.py - DAG Airflow: Ingestão horária de vendas

DIFERENÇA EM RELAÇÃO AO CLIENTES:
    Clientes: ingestão diária (dados mudam pouco)
    Vendas: ingestão horária (vendas acontecem a qualquer momento do dia)

    No AWS real:
    - Vendas em tempo real via Kinesis (streaming)
    - Vendas batch via SQS + Lambda (micro-batch a cada hora)

    Aqui simulamos o batch horário via Airflow.

CONCEITO AIRFLOW - CATCHUP:
    catchup=False: Se o Airflow ficou offline por 3 horas, ao voltar,
    ele NÃO executa as 3 execuções perdidas.

    catchup=True: Executaria as 3 execuções "atrasadas" automaticamente.
    Útil para reprocessamento histórico.

    Para vendas, catchup=True faz sentido (queremos todos os dados).
    Para alertas em tempo real, catchup=False (alerta de 3h atrás não tem valor).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'time-dados',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['alertas@minha-empresa.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=1),
}


def verificar_dados_disponiveis(**context):
    """
    Task 1: Verifica se há novos dados de vendas para ingerir.

    BranchPythonOperator: Decide qual caminho seguir no DAG.
    Se não há dados → branch 'sem_dados_novos' (pula ingestão)
    Se há dados → branch 'processar_vendas' (continua normalmente)

    Isso evita execuções desnecessárias quando não há vendas.
    """
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    from s3.scripts.s3_utils import S3Local
    s3 = S3Local(str(projeto_root / 's3'))

    # Verifica arquivos de vendas na pasta raw
    objetos = s3.list_objects('raw/vendas/')
    arquivos_csv = [o for o in objetos if o['Key'].endswith('.csv')]

    print(f"Arquivos CSV de vendas encontrados: {len(arquivos_csv)}")

    context['ti'].xcom_push(key='qtd_arquivos', value=len(arquivos_csv))

    if not arquivos_csv:
        print("Nenhum dado novo. Pulando ingestão.")
        return 'sem_dados_novos'  # Branch: vai para task 'sem_dados_novos'

    return 'ingerir_vendas'  # Branch: vai para task 'ingerir_vendas'


def ingerir_vendas_da_api(**context):
    """
    Task 2a: Simula consumo da API de vendas.

    No AWS real, aqui o Airbyte faria:
        - Source: REST API (sistema de vendas)
        - Destination: S3 (s3://bucket/raw/vendas/)
        - Modo: Incremental (só novas vendas desde a última sync)

    Parâmetros importantes:
        - execution_date: data/hora desta execução do Airflow
        - prev_execution_date: data/hora da execução anterior
        Isso define o período incremental.
    """
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    execution_date = context['execution_date']
    prev_execution_date = context.get('prev_execution_date') or (execution_date - timedelta(hours=1))

    print(f"Período de ingestão: {prev_execution_date} → {execution_date}")
    print(f"Tipo: Incremental (apenas novas vendas no período)")

    from s3.scripts.s3_utils import S3Local
    s3 = S3Local(str(projeto_root / 's3'))

    # Verifica dados disponíveis
    objetos = s3.list_objects('raw/vendas/')
    print(f"Dados disponíveis em raw/vendas/: {len(objetos)} arquivos")

    context['ti'].xcom_push(key='ingestao_concluida', value=True)
    context['ti'].xcom_push(key='periodo_inicio', value=prev_execution_date.isoformat())
    context['ti'].xcom_push(key='periodo_fim', value=execution_date.isoformat())


def publicar_na_fila_sqs(**context):
    """
    Task 3: Publica notificação na fila SQS de que novos dados chegaram.

    No AWS: o Lambda processaria essa mensagem para:
        - Notificar outros sistemas que há dados novos
        - Disparar processamento downstream (Glue job)
        - Atualizar dashboards em tempo real

    Conceito SQS:
        Desacopla o produtor (ingestão) do consumidor (ETL).
        O ETL não precisa esperar a ingestão - processa quando estiver pronto.
    """
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    from sqs.sqs_local import SQSLocal

    sqs = SQSLocal()

    ti = context['ti']

    mensagem = {
        'tipo': 'NOVOS_DADOS_VENDAS',
        'dag': context['dag'].dag_id,
        'execution_date': context['ds'],
        'periodo_inicio': ti.xcom_pull(task_ids='ingerir_vendas', key='periodo_inicio'),
        'periodo_fim': ti.xcom_pull(task_ids='ingerir_vendas', key='periodo_fim'),
        'timestamp': datetime.now().isoformat()
    }

    resultado = sqs.send_message('vendas_queue', mensagem)
    print(f"Mensagem publicada no SQS. MessageId: {resultado['MessageId']}")

    context['ti'].xcom_push(key='sqs_message_id', value=resultado['MessageId'])


def sem_dados_handler(**context):
    """Handler para quando não há dados novos."""
    print("Nenhum dado novo de vendas. Execução pulada.")
    print(f"Próxima execução: {context['next_execution_date']}")


# =============================================================================
# DEFINIÇÃO DO DAG COM BRANCHING
# =============================================================================
with DAG(
    dag_id='dag_ingestion_vendas',
    description='Ingestão horária de vendas via API + SQS notification',
    default_args=DEFAULT_ARGS,
    schedule='0 * * * *',       # Todo começo de hora (00:00, 01:00, 02:00, ...)
    catchup=True,               # Recupera execuções perdidas (queremos todos os dados)
    max_active_runs=3,          # Pode ter até 3 execuções simultâneas (horário alto volume)
    tags=['ingestion', 'vendas', 'sqs', 'incremental'],
    doc_md="""
    ## DAG: Ingestão Horária de Vendas

    **Frequência**: A cada hora
    **Tipo**: Incremental (só novas vendas do período)

    **Pipeline**:
    1. Verifica se há dados novos (branch)
    2a. Se sim: Ingestão via API → S3 raw/vendas/
    2b. Se não: Log e encerra
    3. Publica notificação no SQS

    **Catchup**: True - recupera execuções perdidas automaticamente.
    """
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    # BranchPythonOperator: decide o fluxo baseado em condição
    # IMPORTANTE: Deve retornar o task_id para executar (ou lista de task_ids)
    verificar_dados = BranchPythonOperator(
        task_id='verificar_dados_disponiveis',
        python_callable=verificar_dados_disponiveis,
        doc_md="Verifica se há dados novos. Define o branch do pipeline."
    )

    # Branch 1: Há dados para ingerir
    ingerir_vendas = PythonOperator(
        task_id='ingerir_vendas',
        python_callable=ingerir_vendas_da_api,
        doc_md="Consome API de vendas e salva em S3 raw/vendas/."
    )

    publicar_sqs = PythonOperator(
        task_id='publicar_notificacao_sqs',
        python_callable=publicar_na_fila_sqs,
        doc_md="Publica mensagem no SQS notificando novos dados disponíveis."
    )

    # Branch 2: Sem dados novos
    sem_dados = PythonOperator(
        task_id='sem_dados_novos',
        python_callable=sem_dados_handler,
        doc_md="Registra que não há dados novos neste período."
    )

    # trigger_rule='none_failed_min_one_success': O fim roda se pelo menos
    # um branch completou (independente de qual)
    fim = EmptyOperator(
        task_id='fim',
        trigger_rule='none_failed_min_one_success'
    )

    # Dependências com branching
    inicio >> verificar_dados
    verificar_dados >> ingerir_vendas >> publicar_sqs >> fim
    verificar_dados >> sem_dados >> fim
