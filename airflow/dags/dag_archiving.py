"""
dag_archiving.py - DAG Airflow: Arquivamento semanal de dados antigos

CONCEITO:
    Arquivamento automático = S3 Lifecycle Policies localmente.
    No AWS real, o S3 faz isso automaticamente sem precisar do Airflow.
    Aqui usamos Airflow para ter controle, logs e alertas do processo.

Schedule: Domingo às 01:00 (baixo uso do sistema)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'time-dados',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=4),
}


def executar_lifecycle_check(**context):
    """Task: Verifica arquivos candidatos ao arquivamento (dry-run)."""
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    from s3.scripts.lifecycle_manager import LifecycleManager

    manager = LifecycleManager()
    pendentes = manager.get_files_pending_action()

    total_para_arquivar = len(pendentes.get('arquivar_em_breve', []))
    total_para_deletar = len(pendentes.get('deletar_em_breve', []))

    print(f"Arquivos para arquivar em breve: {total_para_arquivar}")
    print(f"Arquivos para deletar em breve : {total_para_deletar}")

    context['ti'].xcom_push(key='para_arquivar', value=total_para_arquivar)
    context['ti'].xcom_push(key='para_deletar', value=total_para_deletar)


def executar_archiving(**context):
    """Task: Executa o arquivamento (move arquivos antigos)."""
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    from s3.scripts.lifecycle_manager import LifecycleManager

    manager = LifecycleManager()
    stats = manager.apply_rules(dry_run=False, verbose=False)

    print(f"Arquivamento concluído:")
    print(f"  Arquivados: {stats['arquivos_arquivados']}")
    print(f"  Deletados : {stats['arquivos_deletados']}")
    print(f"  Dados movidos: {stats['bytes_movidos']/1024/1024:.2f} MB")

    context['ti'].xcom_push(key='archiving_stats', value=stats)


def invocar_lambda_archive(**context):
    """Task: Invoca a Lambda de arquivamento para arquivos > 90 dias."""
    import sys
    from pathlib import Path

    projeto_root = Path('/opt/data_platform')
    if not projeto_root.exists():
        projeto_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(projeto_root))

    from lambda.functions.archive_old_data import lambda_handler

    resultado = lambda_handler({
        'dias_limite': 90,
        'prefixo': 'raw/',
        'dry_run': False
    })

    print(f"Lambda archive_old_data: {resultado['body']['arquivos_arquivados']} arquivos")
    context['ti'].xcom_push(key='lambda_result', value=resultado['body'])


def gerar_relatorio_archiving(**context):
    """Task: Gera relatório de archiving para o time."""
    import json
    from pathlib import Path

    ti = context['ti']

    archiving_stats = ti.xcom_pull(task_ids='executar_archiving', key='archiving_stats') or {}
    lambda_result = ti.xcom_pull(task_ids='invocar_lambda_archive', key='lambda_result') or {}

    relatorio = {
        'data': context['ds'],
        'semana': datetime.now().strftime('%Y-W%W'),
        'archiving': archiving_stats,
        'lambda_archive': lambda_result,
        'timestamp': datetime.now().isoformat()
    }

    print(f"RELATÓRIO DE ARCHIVING - {context['ds']}")
    print(json.dumps(relatorio, indent=2, ensure_ascii=False, default=str))


with DAG(
    dag_id='dag_archiving',
    description='Arquivamento semanal de dados antigos (S3 Lifecycle Simulation)',
    default_args=DEFAULT_ARGS,
    schedule='0 1 * * 0',      # Todo domingo às 01:00
    catchup=False,
    max_active_runs=1,
    tags=['archiving', 'lifecycle', 'manutencao'],
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    verificar_lifecycle = PythonOperator(
        task_id='verificar_lifecycle',
        python_callable=executar_lifecycle_check,
        doc_md="Verifica arquivos candidatos ao arquivamento (dry-run)"
    )

    arquivar = PythonOperator(
        task_id='executar_archiving',
        python_callable=executar_archiving,
        doc_md="Move arquivos antigos de raw/ para archived/ (simula S3 Glacier)"
    )

    lambda_archive = PythonOperator(
        task_id='invocar_lambda_archive',
        python_callable=invocar_lambda_archive,
        doc_md="Invoca Lambda archive_old_data para arquivos > 90 dias"
    )

    relatorio = PythonOperator(
        task_id='gerar_relatorio',
        python_callable=gerar_relatorio_archiving,
        doc_md="Gera relatório semanal de archiving"
    )

    fim = EmptyOperator(task_id='fim')

    inicio >> verificar_lifecycle >> arquivar >> lambda_archive >> relatorio >> fim
