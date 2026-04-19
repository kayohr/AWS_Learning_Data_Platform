"""
dag_ingestion_clientes.py - DAG Airflow: Ingestão diária de clientes

CONCEITO AWS MWAA:
    No AWS MWAA (Managed Workflows for Apache Airflow), este DAG seria
    exatamente igual. A diferença é onde roda:
        Local: Apache Airflow em Docker
        AWS:   MWAA (Airflow gerenciado, sem gerenciar servidores)

    Custo MWAA: ~$0.49/ambiente-hora + $0.49/worker-hora

PIPELINE:
    CRM (PostgreSQL) → Airbyte → s3/raw/clientes/ → Lambda Validação

    Schedule: Todo dia às 02:00 (horário de Brasília)

    Por que às 02:00?
    - Menor uso do sistema de origem (CRM)
    - Dados do dia anterior completos
    - Prontos para o ETL das 04:00
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


# =============================================================================
# CONFIGURAÇÕES DO DAG
# =============================================================================

# Configurações padrão das tasks
# On_failure_callback: chamado quando uma task falha
# On_success_callback: chamado quando o DAG completa com sucesso
DEFAULT_ARGS = {
    'owner': 'time-dados',
    'depends_on_past': False,       # Não depende da execução anterior
    'start_date': days_ago(1),      # Começa ontem (para poder executar imediatamente)
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alertas@minha-empresa.com'],
    'retries': 3,                   # Tenta 3 vezes em caso de falha
    'retry_delay': timedelta(minutes=5),  # Espera 5 min entre tentativas
    'retry_exponential_backoff': True,    # Aumenta delay a cada retry
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),  # Timeout por task
}


# =============================================================================
# FUNÇÕES DAS TASKS
# =============================================================================

def verificar_conectividade_crm(**context):
    """
    Task 1: Verifica se o CRM (banco de dados de origem) está acessível.

    No AWS real: verificaria a conectividade com o banco RDS ou on-premises
    via VPC/VPN antes de iniciar a ingestão.

    Conceito Airflow: tasks de verificação de pré-condição são boa prática.
    Se esta task falha, as demais não executam (evita erro em cascade).
    """
    import psycopg2
    import os

    print("Verificando conectividade com o CRM (banco de dados de origem)...")

    # Configurações de conexão do CRM
    # No AWS: seria armazenado no Secrets Manager ou SSM Parameter Store
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = int(os.getenv('POSTGRES_PORT', 5432))
    database = os.getenv('POSTGRES_DB', 'dataplatform')
    user = os.getenv('POSTGRES_USER', 'dataplatform_user')
    password = os.getenv('POSTGRES_PASSWORD', 'dataplatform_pass123')

    try:
        conn = psycopg2.connect(
            host=host, port=port, database=database,
            user=user, password=password,
            connect_timeout=10
        )
        cur = conn.cursor()
        cur.execute("SELECT version()")
        versao = cur.fetchone()[0]
        print(f"CRM conectado! PostgreSQL: {versao[:50]}...")
        conn.close()

        # XCom: passa informações para tasks seguintes
        # No Airflow, use XCom para compartilhar dados entre tasks
        context['ti'].xcom_push(key='crm_conectado', value=True)
        context['ti'].xcom_push(key='conexao_testada_em', value=datetime.now().isoformat())

    except Exception as e:
        print(f"ERRO: CRM inacessível: {e}")
        raise Exception(f"Falha na conectividade com CRM: {e}")


def executar_airbyte_sync(**context):
    """
    Task 2: Dispara sincronização do Airbyte (CRM → S3 raw/clientes/).

    CONCEITO AIRBYTE:
        No Airbyte real, você chamaria a API REST:
            POST /api/v1/connections/sync
            Body: {"connectionId": "uuid-da-conexao"}

        Aqui simulamos gerando dados diretamente para s3/raw/clientes/
        (já que não temos o Airbyte completo rodando em Docker para simplificar).

    No AWS real com DMS (Database Migration Service):
        - Configurado via console: DMS → Replication tasks → Create task
        - Tipo: Full load + CDC (Change Data Capture)
        - Destino: S3 com formato JSON ou CSV
    """
    import sys
    import os
    from pathlib import Path

    print("Iniciando sincronização Airbyte: CRM → S3 raw/clientes/")

    # Simula a chamada ao Airbyte gerando dados de exemplo
    # Em produção: seria uma chamada HTTP à API do Airbyte
    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent

    sys.path.insert(0, str(projeto_root))

    # Verifica se já há dados; se não, sugere gerar
    from s3.scripts.s3_utils import S3Local
    s3 = S3Local(str(projeto_root / 's3'))

    objetos = s3.list_objects('raw/clientes/')
    print(f"Arquivos encontrados em raw/clientes/: {len(objetos)}")

    if not objetos:
        print("AVISO: Nenhum arquivo em raw/clientes/")
        print("Execute: python scripts/generate_sample_data.py")

    # Registra resultado via XCom
    context['ti'].xcom_push(key='arquivos_raw', value=len(objetos))
    context['ti'].xcom_push(key='sync_concluido_em', value=datetime.now().isoformat())

    print(f"Sync concluído. {len(objetos)} arquivo(s) disponíveis.")


def validar_schema_clientes(**context):
    """
    Task 3: Valida schema dos arquivos recém-chegados.

    No AWS real: Esta seria uma Lambda invocada automaticamente pelo S3 trigger.
    No Airflow: chamamos a função Lambda explicitamente como uma task.

    Conceito: Validação de schema ANTES do ETL evita falhas tardias e
    caras (um job Glue de 30min que falha no final por dado inválido).
    """
    import sys
    import json
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent

    sys.path.insert(0, str(projeto_root))

    print("Validando schema dos arquivos de clientes...")

    from s3.scripts.s3_utils import S3Local
    s3 = S3Local(str(projeto_root / 's3'))

    # Cria um evento S3 simulado para cada arquivo em raw/clientes/
    objetos = s3.list_objects('raw/clientes/')

    if not objetos:
        print("Nenhum arquivo para validar.")
        context['ti'].xcom_push(key='validacao_ok', value=True)
        return

    # Importa o Lambda de validação
    from lambda.functions.validate_schema import lambda_handler

    arquivos_invalidos = []

    for obj in objetos[:10]:  # Valida até 10 arquivos por execução
        evento = {
            "Records": [{
                "s3": {
                    "bucket": {"name": "data-platform-local"},
                    "object": {"key": obj['Key'], "size": obj['Size']}
                }
            }]
        }

        try:
            resultado = lambda_handler(evento)
            if resultado['statusCode'] != 200:
                arquivos_invalidos.append(obj['Key'])
                print(f"INVÁLIDO: {obj['Key']}")
            else:
                print(f"VÁLIDO: {obj['Key']}")
        except Exception as e:
            print(f"ERRO ao validar {obj['Key']}: {e}")
            arquivos_invalidos.append(obj['Key'])

    if arquivos_invalidos:
        context['ti'].xcom_push(key='validacao_ok', value=False)
        context['ti'].xcom_push(key='arquivos_invalidos', value=arquivos_invalidos)
        raise ValueError(
            f"Validação falhou para {len(arquivos_invalidos)} arquivo(s): {arquivos_invalidos}"
        )

    context['ti'].xcom_push(key='validacao_ok', value=True)
    print("Todos os arquivos validados com sucesso!")


def registrar_metricas_ingestion(**context):
    """
    Task 4: Registra métricas da ingestão para monitoramento.

    No AWS: Enviaria métricas para CloudWatch:
        cloudwatch.put_metric_data(
            Namespace='DataPlatform/Ingestion',
            MetricData=[{
                'MetricName': 'ClientesIngeridos',
                'Value': qtd_registros,
                'Unit': 'Count'
            }]
        )

    Localmente: Salva em arquivo de monitoramento.
    """
    import json
    from pathlib import Path

    # Coleta métricas de XComs das tasks anteriores
    ti = context['ti']

    metricas = {
        'dag': context['dag'].dag_id,
        'data_execucao': context['ds'],  # ds = execution date string (YYYY-MM-DD)
        'timestamp': datetime.now().isoformat(),
        'crm_conectado': ti.xcom_pull(task_ids='verificar_conectividade', key='crm_conectado'),
        'arquivos_raw': ti.xcom_pull(task_ids='executar_sync_airbyte', key='arquivos_raw'),
        'validacao_ok': ti.xcom_pull(task_ids='validar_schema', key='validacao_ok'),
    }

    print(f"Métricas da ingestão: {json.dumps(metricas, indent=2)}")

    # Salva métricas localmente
    metrics_dir = Path('/opt/data_platform/monitoring/metrics')
    if not metrics_dir.exists():
        metrics_dir = Path(__file__).parent.parent.parent / 'monitoring' / 'metrics'
    metrics_dir.mkdir(parents=True, exist_ok=True)

    data = context['ds'].replace('-', '')
    with open(metrics_dir / f"ingestion_clientes_{data}.json", 'w') as f:
        json.dump(metricas, f, ensure_ascii=False, indent=2)

    print("Métricas registradas com sucesso!")


# =============================================================================
# DEFINIÇÃO DO DAG
# =============================================================================
# O DAG é definido como context manager (with DAG(...) as dag:)
# Todas as tasks dentro do bloco pertencem a este DAG.
# =============================================================================

with DAG(
    dag_id='dag_ingestion_clientes',                # ID único do DAG
    description='Ingestão diária de clientes do CRM para o S3 raw',
    default_args=DEFAULT_ARGS,
    schedule='0 2 * * *',                           # Todo dia às 02:00
    catchup=False,                                  # Não executa datas passadas ao ativar
    max_active_runs=1,                              # Só 1 execução simultânea
    tags=['ingestion', 'clientes', 'airbyte'],      # Tags para filtrar na UI
    doc_md="""
    ## DAG: Ingestão de Clientes

    **Frequência**: Diária às 02:00 (horário de Brasília)

    **Pipeline**:
    1. Verifica conectividade com o CRM
    2. Dispara sincronização Airbyte (CRM → S3 raw/clientes/)
    3. Valida schema dos arquivos recebidos
    4. Registra métricas de monitoramento

    **Em caso de falha**: 3 retries com backoff exponencial.
    Alerta enviado por e-mail e Slack após falha definitiva.

    **Dependências**: Este DAG deve completar antes do dag_etl_full_pipeline.
    """
) as dag:

    # -------------------------------------------------------------------------
    # TASK 0: Marcador de início
    # EmptyOperator: não faz nada, mas cria um ponto de entrada visual no DAG
    # -------------------------------------------------------------------------
    inicio = EmptyOperator(
        task_id='inicio',
        doc_md="Ponto de entrada do pipeline de ingestão de clientes."
    )

    # -------------------------------------------------------------------------
    # TASK 1: Verifica conectividade com o CRM
    # -------------------------------------------------------------------------
    verificar_conectividade = PythonOperator(
        task_id='verificar_conectividade',
        python_callable=verificar_conectividade_crm,
        doc_md="Verifica se o banco de dados CRM está acessível antes de iniciar."
    )

    # -------------------------------------------------------------------------
    # TASK 2: Sincronização via Airbyte
    # -------------------------------------------------------------------------
    executar_sync_airbyte = PythonOperator(
        task_id='executar_sync_airbyte',
        python_callable=executar_airbyte_sync,
        doc_md="Dispara sincronização Airbyte: CRM → S3 raw/clientes/."
    )

    # -------------------------------------------------------------------------
    # TASK 3: Validação de Schema
    # -------------------------------------------------------------------------
    validar_schema = PythonOperator(
        task_id='validar_schema',
        python_callable=validar_schema_clientes,
        doc_md="Valida schema e qualidade dos arquivos usando Lambda validate_schema."
    )

    # -------------------------------------------------------------------------
    # TASK 4: Registra Métricas
    # -------------------------------------------------------------------------
    registrar_metricas = PythonOperator(
        task_id='registrar_metricas',
        python_callable=registrar_metricas_ingestion,
        doc_md="Registra métricas de monitoramento (quantidade de registros, tempo, etc.)."
    )

    # -------------------------------------------------------------------------
    # TASK 5: Marcador de fim
    # -------------------------------------------------------------------------
    fim = EmptyOperator(
        task_id='fim',
        doc_md="Pipeline de ingestão de clientes concluído."
    )

    # =========================================================================
    # DEFINIÇÃO DAS DEPENDÊNCIAS
    # >> define "roda antes de"
    # A >> B significa: "A deve completar com sucesso antes de B iniciar"
    # =========================================================================
    inicio >> verificar_conectividade >> executar_sync_airbyte >> validar_schema >> registrar_metricas >> fim
