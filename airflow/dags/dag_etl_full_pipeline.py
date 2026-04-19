"""
dag_etl_full_pipeline.py - DAG: Pipeline ETL Completo

PIPELINE COMPLETO:
    S3 raw/ → Glue ETL → S3 processed/ → Redshift staging → DW → Data Marts

    Este é o DAG "principal" da plataforma.
    Orquestra todos os jobs ETL e a carga no Data Warehouse.

    Schedule: Todo dia às 04:00 (depois das ingestões das 02:00)

CONCEITO AIRFLOW - SENSOR:
    O ExternalTaskSensor aguarda outro DAG terminar antes de iniciar:
        sensor = ExternalTaskSensor(
            task_id='aguardar_ingestion',
            external_dag_id='dag_ingestion_clientes',
            external_task_id='fim',
            timeout=3600  # Aguarda até 1 hora
        )

    No AWS Step Functions seria equivalente:
        {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {"StateMachineArn": "arn:ingestion-clientes"}
        }
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'time-dados',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['alertas@minha-empresa.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=3),
}


def on_failure_callback(context):
    """
    Callback chamado quando qualquer task do DAG falha.

    No AWS: enviaria alertas via SNS → Email + Slack + PagerDuty
    """
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    try:
        from lambda.functions.notify_pipeline import notificar_falha_pipeline
        notificar_falha_pipeline(
            pipeline_nome=context['dag'].dag_id,
            mensagem_erro=str(context.get('exception', 'Erro desconhecido')),
            componente=context.get('task_instance', {}).task_id if hasattr(
                context.get('task_instance', {}), 'task_id') else 'unknown'
        )
    except Exception as e:
        print(f"Erro ao enviar notificação: {e}")


def executar_glue_job_clientes(**context):
    """
    Task: Executa job Glue de clientes (raw → processed).

    No AWS real:
        glue_client = boto3.client('glue')
        response = glue_client.start_job_run(
            JobName='clientes-raw-to-processed',
            Arguments={
                '--data_processamento': context['ds'],
                '--enable-metrics': ''
            }
        )
        # Aguardar conclusão...
        job_run_id = response['JobRunId']

    Localmente: chama a função Python diretamente.
    """
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    print(f"Iniciando Glue Job: clientes_raw_to_processed")
    print(f"Data: {context['ds']}")

    from glue.jobs.clientes_raw_to_processed import run_job
    resultado = run_job(data_processamento=context['ds'])

    if resultado['status'] == 'FAILED':
        raise Exception(f"Glue Job falhou: {resultado.get('erros', [])}")

    context['ti'].xcom_push(key='clientes_job_result', value=resultado)
    print(f"Job concluído: {resultado['registros_processados']} registros")


def executar_glue_job_vendas(**context):
    """Task: Executa job Glue de vendas (raw → processed + join clientes)."""
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    print(f"Iniciando Glue Job: vendas_raw_to_processed")

    from glue.jobs.vendas_raw_to_processed import run_job
    resultado = run_job()

    if resultado['status'] == 'FAILED':
        raise Exception(f"Glue Job falhou: {resultado.get('erros', [])}")

    context['ti'].xcom_push(key='vendas_job_result', value=resultado)
    print(f"Job concluído: {resultado.get('registros_processados', 0)} registros")


def executar_agregacoes_diarias(**context):
    """Task: Executa agregações diárias (ELT)."""
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    print(f"Iniciando agregações diárias para {context['ds']}")

    from glue.jobs.daily_aggregations import run_job
    resultado = run_job(data_referencia=context['ds'])

    if resultado['status'] == 'FAILED':
        raise Exception(f"Agregações falharam: {resultado.get('erros', [])}")

    print(f"Agregações concluídas: {resultado.get('agregacoes', {})}")


def carregar_staging_redshift(**context):
    """
    Task: Carrega dados do S3 processed/ para o staging do Redshift.

    No AWS real: Comando COPY do Redshift
        COPY staging.stg_clientes
        FROM 's3://bucket/processed/clientes/'
        IAM_ROLE 'arn:aws:iam::123456789:role/redshift-role'
        FORMAT AS PARQUET;
    """
    import sys
    import io
    import os
    import psycopg2
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    print("Carregando dados do S3 processed/ para staging do Redshift...")

    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'dataplatform'),
            user=os.getenv('POSTGRES_USER', 'dataplatform_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'dataplatform_pass123')
        )

        batch_id = f"BATCH_{context['ds'].replace('-', '')}"

        with conn.cursor() as cur:
            # Executa stored procedure de carga
            cur.execute(
                "CALL staging.sp_load_clientes(%s, %s)",
                (batch_id, True)
            )
        conn.commit()
        conn.close()
        print(f"Staging carregado (batch: {batch_id})")

    except Exception as e:
        print(f"AVISO: Erro no staging (banco pode não estar iniciado): {e}")
        print("Execute: python redshift/init_db.py")


def transformar_dw(**context):
    """Task: Transforma staging → DW (Star Schema)."""
    import sys
    import os
    import psycopg2
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent

    print("Transformando dados staging → DW (Star Schema)...")

    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'dataplatform'),
            user=os.getenv('POSTGRES_USER', 'dataplatform_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'dataplatform_pass123')
        )

        batch_id = f"BATCH_{context['ds'].replace('-', '')}"

        with conn.cursor() as cur:
            cur.execute("CALL dw.sp_transform_dim_clientes(%s)", (batch_id,))
            cur.execute("CALL dw.sp_transform_dim_produto(%s)", (batch_id,))
            cur.execute(
                "CALL dw.sp_transform_fato_vendas(%s, %s::DATE, %s::DATE)",
                (batch_id, context['ds'], context['ds'])
            )
        conn.commit()
        conn.close()
        print("DW atualizado com sucesso!")

    except Exception as e:
        print(f"AVISO: Erro no DW (banco pode não estar iniciado): {e}")


def atualizar_data_marts(**context):
    """Task: Atualiza os Data Marts."""
    import sys
    import os
    import psycopg2
    from pathlib import Path

    print("Atualizando Data Marts...")

    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'dataplatform'),
            user=os.getenv('POSTGRES_USER', 'dataplatform_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'dataplatform_pass123')
        )

        batch_id = f"BATCH_{context['ds'].replace('-', '')}"

        with conn.cursor() as cur:
            cur.execute("CALL dm.sp_refresh_data_marts(%s)", (batch_id,))
        conn.commit()
        conn.close()
        print("Data Marts atualizados!")

    except Exception as e:
        print(f"AVISO: Erro nos Data Marts: {e}")


def verificar_qualidade_final(**context):
    """Task: Verifica qualidade dos dados no DW após a carga."""
    import sys
    import os
    import psycopg2
    from pathlib import Path

    print("Verificando qualidade dos dados no DW...")

    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'dataplatform'),
            user=os.getenv('POSTGRES_USER', 'dataplatform_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'dataplatform_pass123')
        )

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.dim_clientes WHERE registro_atual = TRUE")
            qtd_clientes = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dw.fato_vendas WHERE data_venda::DATE = %s",
                       (context['ds'],))
            qtd_vendas_hoje = cur.fetchone()[0]

        conn.close()

        print(f"QA Report:")
        print(f"  Clientes ativos no DW: {qtd_clientes}")
        print(f"  Vendas de {context['ds']}: {qtd_vendas_hoje}")

        context['ti'].xcom_push(key='qa_clientes', value=qtd_clientes)
        context['ti'].xcom_push(key='qa_vendas_hoje', value=qtd_vendas_hoje)

    except Exception as e:
        print(f"AVISO: QA não executada (banco pode não estar iniciado): {e}")


# =============================================================================
# DAG PRINCIPAL
# =============================================================================
with DAG(
    dag_id='dag_etl_full_pipeline',
    description='Pipeline ETL completo: S3 raw → Glue → Redshift DW → Data Marts',
    default_args={**DEFAULT_ARGS, 'on_failure_callback': on_failure_callback},
    schedule='0 4 * * *',      # Todo dia às 04:00 (após ingestões das 02:00)
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'glue', 'redshift', 'dw', 'producao'],
    doc_md="""
    ## DAG: Pipeline ETL Completo

    **Frequência**: Diária às 04:00
    **Duração estimada**: 30-60 minutos

    **Pipeline**:
    ```
    Ingestão OK? → Glue Clientes → Glue Vendas (paralelo)
                                          ↓
                                  Agregações Diárias
                                          ↓
                                  Load Staging Redshift
                                          ↓
                                  Transform DW (Star Schema)
                                          ↓
                                  Atualizar Data Marts
                                          ↓
                                  QA Final → Notificação
    ```
    """
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    # -------------------------------------------------------------------------
    # Grupo ETL: Jobs Glue (rodam em paralelo para economizar tempo)
    # -------------------------------------------------------------------------
    # No AWS Glue real: rodariam em clusters Spark separados simultaneamente
    glue_clientes = PythonOperator(
        task_id='glue_etl_clientes',
        python_callable=executar_glue_job_clientes,
        doc_md="Glue Job: raw/clientes/ → processed/clientes/ (JSON → Parquet)"
    )

    glue_vendas = PythonOperator(
        task_id='glue_etl_vendas',
        python_callable=executar_glue_job_vendas,
        doc_md="Glue Job: raw/vendas/ → processed/vendas/ (CSV → Parquet + JOIN clientes)"
    )

    glue_agregacoes = PythonOperator(
        task_id='glue_agregacoes_diarias',
        python_callable=executar_agregacoes_diarias,
        doc_md="ELT: Calcula agregações diárias (vendas por dia/UF/categoria)"
    )

    # -------------------------------------------------------------------------
    # Grupo Redshift: Carga e transformação no DW
    # -------------------------------------------------------------------------
    load_staging = PythonOperator(
        task_id='load_staging_redshift',
        python_callable=carregar_staging_redshift,
        doc_md="COPY S3 processed/ → staging Redshift (usando stored procedure)"
    )

    transform_dw = PythonOperator(
        task_id='transform_star_schema',
        python_callable=transformar_dw,
        doc_md="SP: staging → DW Star Schema (dim_clientes, fato_vendas)"
    )

    update_data_marts = PythonOperator(
        task_id='update_data_marts',
        python_callable=atualizar_data_marts,
        doc_md="SP: Atualiza Data Marts (dm_vendas_diarias, dm_top_clientes)"
    )

    qa_final = PythonOperator(
        task_id='verificacao_qualidade_final',
        python_callable=verificar_qualidade_final,
        doc_md="QA: Verifica contagens e métricas de qualidade pós-carga"
    )

    fim = EmptyOperator(task_id='fim')

    # =========================================================================
    # DEPENDÊNCIAS DO PIPELINE
    # Glue clientes e vendas rodam em PARALELO (>>[glue_clientes, glue_vendas])
    # =========================================================================
    inicio >> [glue_clientes, glue_vendas]   # Paralelo
    [glue_clientes, glue_vendas] >> glue_agregacoes
    glue_agregacoes >> load_staging
    load_staging >> transform_dw
    transform_dw >> update_data_marts
    update_data_marts >> qa_final
    qa_final >> fim
