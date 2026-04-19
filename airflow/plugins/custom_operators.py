"""
custom_operators.py - Operadores Airflow customizados para a plataforma local

CONCEITO AIRFLOW CUSTOM OPERATORS:
    Você pode criar seus próprios operadores reutilizáveis.
    No AWS, existem operadores prontos como:
        - S3ToRedshiftOperator (apache-airflow-providers-amazon)
        - GlueJobOperator
        - LambdaInvokeFunctionOperator

    Aqui criamos equivalentes locais para fins educacionais.
"""

from datetime import datetime
from typing import Optional, Callable, Dict, Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GlueJobLocalOperator(BaseOperator):
    """
    Operador customizado que simula o GlueJobOperator do AWS.

    Uso no DAG:
        from airflow.plugins.custom_operators import GlueJobLocalOperator

        tarefa = GlueJobLocalOperator(
            task_id='etl_clientes',
            job_function='glue.jobs.clientes_raw_to_processed.run_job',
            job_args={'data_processamento': '{{ ds }}'},
        )

    No AWS real (usando GlueJobOperator):
        from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

        tarefa = GlueJobOperator(
            task_id='etl_clientes',
            job_name='clientes-raw-to-processed',
            script_args={'--data_processamento': '{{ ds }}'},
            aws_conn_id='aws_default',
        )
    """

    # Cor do operador na UI do Airflow (para identificação visual)
    ui_color = '#FF9900'  # Laranja AWS

    @apply_defaults
    def __init__(
        self,
        job_function: str,       # "modulo.submodulo.funcao"
        job_args: Dict = None,
        timeout_minutes: int = 60,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.job_function = job_function
        self.job_args = job_args or {}
        self.timeout_minutes = timeout_minutes

    def execute(self, context):
        """Executa o job Glue local."""
        import importlib

        self.log.info(f"Iniciando Glue Job local: {self.job_function}")
        self.log.info(f"Argumentos: {self.job_args}")

        # Importa a função dinamicamente
        modulo_path, funcao_nome = self.job_function.rsplit('.', 1)
        modulo = importlib.import_module(modulo_path)
        funcao = getattr(modulo, funcao_nome)

        # Renderiza templates Jinja nos argumentos (ex: {{ ds }})
        args_renderizados = {}
        for chave, valor in self.job_args.items():
            if isinstance(valor, str) and '{{' in valor:
                args_renderizados[chave] = context.get(valor.strip('{{ }'), valor)
            else:
                args_renderizados[chave] = valor

        inicio = datetime.now()
        resultado = funcao(**args_renderizados)
        duracao = (datetime.now() - inicio).total_seconds()

        self.log.info(f"Job concluído em {duracao:.1f}s")
        self.log.info(f"Resultado: {resultado}")

        if isinstance(resultado, dict) and resultado.get('status') == 'FAILED':
            raise Exception(f"Glue Job falhou: {resultado.get('erros', [])}")

        return resultado


class S3ToRedshiftLocalOperator(BaseOperator):
    """
    Operador que simula o S3ToRedshiftOperator do AWS.

    Carrega arquivos Parquet do S3 local para o PostgreSQL (simulando COPY do Redshift).

    No AWS real:
        from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

        tarefa = S3ToRedshiftOperator(
            task_id='load_clientes',
            schema='staging',
            table='stg_clientes',
            s3_bucket='meu-bucket',
            s3_key='processed/clientes/',
            redshift_conn_id='redshift_default',
            copy_options=['FORMAT AS PARQUET', 'IAM_ROLE arn:aws:iam::123:role/redshift-role'],
        )
    """

    ui_color = '#4285F4'  # Azul Redshift

    @apply_defaults
    def __init__(
        self,
        s3_prefix: str,
        schema: str,
        table: str,
        truncate: bool = False,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.s3_prefix = s3_prefix
        self.schema = schema
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        import io
        import os
        import sys
        from pathlib import Path

        import pandas as pd
        import psycopg2
        from psycopg2.extras import execute_values

        projeto_root = Path('/opt/data_platform')
        if not projeto_root.exists():
            projeto_root = Path(__file__).parent.parent.parent
        sys.path.insert(0, str(projeto_root))

        from s3.scripts.s3_utils import S3Local

        self.log.info(f"COPY: {self.s3_prefix} → {self.schema}.{self.table}")

        s3 = S3Local(str(projeto_root / 's3'))
        objetos = s3.list_objects(prefix=self.s3_prefix)
        arquivos_parquet = [o for o in objetos if o['Key'].endswith('.parquet')]

        if not arquivos_parquet:
            self.log.warning(f"Nenhum Parquet encontrado em {self.s3_prefix}")
            return 0

        dfs = []
        for obj in arquivos_parquet:
            response = s3.get_object(obj['Key'])
            buffer = io.BytesIO(response['Body'])
            df = pd.read_parquet(buffer)
            dfs.append(df)

        df_total = pd.concat(dfs, ignore_index=True)
        self.log.info(f"Lidos {len(df_total)} registros de {len(arquivos_parquet)} arquivos")

        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'dataplatform'),
            user=os.getenv('POSTGRES_USER', 'dataplatform_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'dataplatform_pass123')
        )

        with conn.cursor() as cur:
            if self.truncate:
                cur.execute(f"TRUNCATE TABLE {self.schema}.{self.table}")

        conn.commit()
        conn.close()

        self.log.info(f"COPY concluído: {len(df_total)} registros → {self.schema}.{self.table}")
        return len(df_total)


class LambdaLocalOperator(BaseOperator):
    """
    Operador que simula o LambdaInvokeFunctionOperator do AWS.

    No AWS real:
        from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

        tarefa = LambdaInvokeFunctionOperator(
            task_id='validar_schema',
            function_name='validate-schema-prod',
            payload=json.dumps({'key': 'raw/clientes/arquivo.json'}),
            aws_conn_id='aws_default',
        )
    """

    ui_color = '#FF6B35'  # Laranja Lambda

    @apply_defaults
    def __init__(
        self,
        lambda_handler_path: str,  # "lambda.functions.validate_schema.lambda_handler"
        payload: Dict = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.lambda_handler_path = lambda_handler_path
        self.payload = payload or {}

    def execute(self, context):
        import importlib

        modulo_path, handler_nome = self.lambda_handler_path.rsplit('.', 1)
        modulo = importlib.import_module(modulo_path)
        handler = getattr(modulo, handler_nome)

        self.log.info(f"Invocando Lambda: {self.lambda_handler_path}")

        resultado = handler(self.payload)

        if resultado.get('statusCode', 200) >= 400:
            raise Exception(f"Lambda retornou erro: {resultado}")

        return resultado
