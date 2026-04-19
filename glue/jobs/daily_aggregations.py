"""
daily_aggregations.py - Job Glue: Agregações diárias de vendas

CONCEITO ELT vs ETL:
    ETL (Extract-Transform-Load): Transforma ANTES de carregar no DW
        → clientes_raw_to_processed.py faz isso

    ELT (Extract-Load-Transform): Carrega PRIMEIRO, transforma NO DW
        → Este script faz ELT: lê do S3 processed/ e agrega direto no PostgreSQL/Redshift

    No AWS Redshift: O ELT usa o poder computacional do Redshift
    para fazer as transformações via SQL (muito mais eficiente para grandes volumes).

    No Glue: Você pode usar tanto ETL quanto ELT:
        ETL: processa no cluster Glue (Spark)
        ELT: gera o SQL e roda no Redshift via JDBC

PIPELINE:
    INPUT:  s3/processed/vendas/**/*.parquet  (vendas processadas)
    OUTPUT: PostgreSQL (schema 'dm') → tabelas de data marts para BI
"""

import sys
import logging
import io
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional
import os

import pandas as pd
import pyarrow.parquet as pq

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

from s3.scripts.s3_utils import S3Local

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [GLUE-AGG] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('GlueJob-Aggregations')


def carregar_vendas_processed(s3: S3Local, data_referencia: str = None) -> pd.DataFrame:
    """
    Carrega vendas processadas do S3. Opcionalmente filtra por data.

    No AWS Athena ou Redshift Spectrum, seria:
        SELECT * FROM processed.vendas
        WHERE ano_venda = 2024 AND mes_venda = 1
    """
    prefix = 'processed/vendas/'

    if data_referencia:
        dt = datetime.strptime(data_referencia, '%Y-%m-%d')
        prefix = f"processed/vendas/ano={dt.year}/mes={str(dt.month).zfill(2)}/"

    objetos = s3.list_objects(prefix=prefix)
    arquivos_parquet = [obj for obj in objetos if obj['Key'].endswith('.parquet')]

    if not arquivos_parquet:
        logger.warning(f"Nenhum Parquet encontrado em {prefix}")
        return pd.DataFrame()

    dfs = []
    for obj in arquivos_parquet:
        response = s3.get_object(obj['Key'])
        buffer = io.BytesIO(response['Body'])
        df = pd.read_parquet(buffer)
        dfs.append(df)

    df_total = pd.concat(dfs, ignore_index=True)
    logger.info(f"Carregadas {len(df_total)} vendas de {len(arquivos_parquet)} arquivos")
    return df_total


def agregar_por_dia_uf(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega vendas por dia e UF (estado).

    SQL equivalente no Redshift:
        SELECT
            DATE(data_venda)    AS data_venda,
            uf                  AS estado,
            COUNT(*)            AS qtd_vendas,
            SUM(valor_total)    AS total_vendas,
            AVG(valor_total)    AS ticket_medio,
            MIN(valor_total)    AS menor_venda,
            MAX(valor_total)    AS maior_venda
        FROM dw.fato_vendas
        GROUP BY 1, 2
        ORDER BY 1, 2;

    Returns:
        DataFrame com uma linha por dia/UF
    """
    if df.empty or 'data_venda' not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df['data_venda'] = pd.to_datetime(df['data_venda'])
    df['data'] = df['data_venda'].dt.date

    colunas_group = ['data']
    if 'uf' in df.columns:
        colunas_group.append('uf')

    agg = df.groupby(colunas_group, dropna=False).agg(
        qtd_vendas=('valor_total', 'count'),
        total_vendas=('valor_total', 'sum'),
        ticket_medio=('valor_total', 'mean'),
        menor_venda=('valor_total', 'min'),
        maior_venda=('valor_total', 'max'),
    ).reset_index()

    agg['ticket_medio'] = agg['ticket_medio'].round(2)
    agg['total_vendas'] = agg['total_vendas'].round(2)
    agg['_agg_timestamp'] = datetime.now().isoformat()

    logger.info(f"  Agregação por dia/UF: {len(agg)} linhas")
    return agg


def agregar_por_categoria_produto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega vendas por categoria de produto e mês.

    SQL equivalente no Redshift:
        SELECT
            DATE_TRUNC('month', data_venda) AS mes,
            categoria_produto,
            COUNT(*)                         AS qtd_vendas,
            SUM(valor_total)                AS total_vendas,
            AVG(margem_pct)                 AS margem_media
        FROM dw.fato_vendas
        GROUP BY 1, 2;
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df['data_venda'] = pd.to_datetime(df['data_venda'], errors='coerce')
    df['mes'] = df['data_venda'].dt.to_period('M').astype(str)

    colunas_group = ['mes']
    if 'categoria_produto' in df.columns:
        colunas_group.append('categoria_produto')
    if 'categoria_valor' in df.columns:
        colunas_group.append('categoria_valor')

    if len(colunas_group) == 1:
        return pd.DataFrame()

    colunas_agg = {'qtd_vendas': ('valor_total', 'count'),
                   'total_vendas': ('valor_total', 'sum')}

    if 'margem_pct' in df.columns:
        colunas_agg['margem_media'] = ('margem_pct', 'mean')

    agg = df.groupby(colunas_group, dropna=False).agg(**colunas_agg).reset_index()
    agg['total_vendas'] = agg['total_vendas'].round(2)
    agg['_agg_timestamp'] = datetime.now().isoformat()

    logger.info(f"  Agregação por categoria: {len(agg)} linhas")
    return agg


def agregar_top_clientes(df: pd.DataFrame, top_n: int = 100) -> pd.DataFrame:
    """
    Identifica os top N clientes por volume de compras.

    SQL equivalente:
        SELECT
            cpf_cliente,
            nome_cliente,
            COUNT(*)         AS qtd_pedidos,
            SUM(valor_total) AS total_gasto,
            AVG(valor_total) AS ticket_medio,
            MAX(data_venda)  AS ultima_compra
        FROM dw.fato_vendas
        GROUP BY cpf_cliente, nome_cliente
        ORDER BY total_gasto DESC
        LIMIT 100;
    """
    if df.empty:
        return pd.DataFrame()

    cpf_col = 'cpf_cliente' if 'cpf_cliente' in df.columns else 'cpf'
    nome_col = 'nome' if 'nome' in df.columns else None

    if cpf_col not in df.columns:
        return pd.DataFrame()

    colunas_group = [cpf_col]
    if nome_col:
        colunas_group.append(nome_col)

    colunas_agg = {
        'qtd_pedidos': ('valor_total', 'count'),
        'total_gasto': ('valor_total', 'sum'),
        'ticket_medio': ('valor_total', 'mean'),
    }

    if 'data_venda' in df.columns:
        df['data_venda'] = pd.to_datetime(df['data_venda'], errors='coerce')
        colunas_agg['ultima_compra'] = ('data_venda', 'max')

    agg = df.groupby(colunas_group, dropna=False).agg(**colunas_agg).reset_index()
    agg['total_gasto'] = agg['total_gasto'].round(2)
    agg['ticket_medio'] = agg['ticket_medio'].round(2)

    # Ordena por total gasto (os maiores clientes primeiro)
    agg = agg.sort_values('total_gasto', ascending=False).head(top_n)
    agg['ranking'] = range(1, len(agg) + 1)
    agg['_agg_timestamp'] = datetime.now().isoformat()

    logger.info(f"  Top {top_n} clientes calculados")
    return agg


def salvar_agregacoes(s3: S3Local, df: pd.DataFrame,
                      nome_agregacao: str, data_ref: str) -> str:
    """
    Salva uma agregação como JSON no S3 processed/.

    No AWS real, as agregações seriam carregadas diretamente no Redshift:
        COPY dm.vendas_por_dia_uf
        FROM 's3://bucket/processed/agregacoes/vendas_por_dia_uf/'
        IAM_ROLE 'arn:aws:iam::123456789:role/redshift-s3-role'
        FORMAT AS PARQUET;
    """
    if df.empty:
        logger.warning(f"  DataFrame vazio para {nome_agregacao}, pulando...")
        return None

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f"processed/agregacoes/{nome_agregacao}/{data_ref}/{nome_agregacao}_{timestamp}.json"

    # Converte para JSON (datas precisam ser strings)
    df_json = df.copy()
    for col in df_json.select_dtypes(include=['datetime64', 'object']).columns:
        df_json[col] = df_json[col].astype(str)

    dados_json = df_json.to_dict(orient='records')
    s3.put_object(key, dados_json, metadata={'agregacao': nome_agregacao, 'data': data_ref})

    logger.info(f"  Salvo: {key} ({len(df)} linhas)")
    return key


def run_job(data_referencia: str = None) -> Dict:
    """Executa todas as agregações diárias."""
    if data_referencia is None:
        data_referencia = datetime.now().strftime('%Y-%m-%d')

    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("INICIANDO JOB: daily_aggregations")
    logger.info(f"Data de referência: {data_referencia}")
    logger.info("=" * 60)

    s3 = S3Local()
    resultado = {
        'job_name': 'daily_aggregations',
        'data_referencia': data_referencia,
        'status': 'RUNNING',
        'agregacoes': {},
        'erros': []
    }

    try:
        # Carrega dados de vendas processadas
        df_vendas = carregar_vendas_processed(s3, data_referencia)

        if df_vendas.empty:
            logger.warning("Nenhum dado de vendas para agregar!")
            resultado['status'] = 'SUCCESS_NO_DATA'
            return resultado

        # Executa as agregações
        agregacoes = {
            'vendas_por_dia_uf': agregar_por_dia_uf(df_vendas),
            'vendas_por_categoria': agregar_por_categoria_produto(df_vendas),
            'top_clientes': agregar_top_clientes(df_vendas, top_n=100),
        }

        # Salva cada agregação
        for nome, df_agg in agregacoes.items():
            key = salvar_agregacoes(s3, df_agg, nome, data_referencia)
            resultado['agregacoes'][nome] = {
                'key': key,
                'rows': len(df_agg) if not df_agg.empty else 0
            }

        duracao = (datetime.now() - inicio).total_seconds()
        resultado['status'] = 'SUCCESS'
        resultado['duracao_segundos'] = duracao

        logger.info(f"\nJOB CONCLUÍDO em {duracao:.2f}s")
        for nome, info in resultado['agregacoes'].items():
            logger.info(f"  {nome}: {info['rows']} linhas")

    except Exception as e:
        logger.error(f"ERRO: {e}", exc_info=True)
        resultado['status'] = 'FAILED'
        resultado['erros'].append(str(e))

    return resultado


if __name__ == '__main__':
    resultado = run_job()
    sys.exit(0 if resultado['status'] in ('SUCCESS', 'SUCCESS_NO_DATA') else 1)
