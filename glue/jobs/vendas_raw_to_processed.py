"""
vendas_raw_to_processed.py - Job Glue: Transforma vendas de raw para processed

PIPELINE:
    INPUT:  s3/raw/vendas/**/*.csv      (dados brutos do sistema de vendas)
            s3/processed/clientes/**    (dados já processados de clientes)
    OUTPUT: s3/processed/vendas/        (Parquet enriquecido com dados do cliente)

    Transformações:
        1. Lê CSVs da zona raw/vendas/
        2. Valida valores monetários (R$)
        3. Lê clientes processados para enriquecer as vendas (JOIN)
        4. Calcula campos derivados (valor_com_desconto, categoria_valor)
        5. Particiona por ano/mês para otimizar queries
        6. Salva como Parquet
"""

import os
import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

from s3.scripts.s3_utils import S3Local

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [GLUE-JOB] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('GlueJob-Vendas')

# Configurações do Job
JOB_CONFIG = {
    'input_prefix_vendas': 'raw/vendas/',
    'input_prefix_clientes': 'processed/clientes/',
    'output_prefix': 'processed/vendas/',
    'output_format': 'parquet',
    'compression': 'snappy',
}

# Categorias de valor de venda
CATEGORIAS_VALOR = [
    (0, 100, 'PEQUENA'),
    (100, 500, 'MEDIA'),
    (500, 2000, 'GRANDE'),
    (2000, float('inf'), 'PREMIUM')
]


def limpar_valor_monetario(valor) -> Optional[float]:
    """
    Converte string de valor monetário brasileiro para float.

    Exemplos:
        'R$ 1.250,99' → 1250.99
        '1.250,99'    → 1250.99
        '1250.99'     → 1250.99
        '-'           → None
    """
    if pd.isna(valor) or valor == '-' or valor == '':
        return None

    if isinstance(valor, (int, float)):
        return float(valor)

    # Remove símbolo de moeda e espaços
    valor_str = str(valor).strip()
    valor_str = re.sub(r'R\$\s*', '', valor_str)
    valor_str = valor_str.strip()

    # Formato brasileiro: 1.250,99 → 1250.99
    if ',' in valor_str and '.' in valor_str:
        # Remove pontos de milhar e converte vírgula decimal
        valor_str = valor_str.replace('.', '').replace(',', '.')
    elif ',' in valor_str:
        # Só vírgula: é o decimal
        valor_str = valor_str.replace(',', '.')

    try:
        resultado = float(valor_str)
        return resultado if resultado >= 0 else None
    except ValueError:
        return None


def categorizar_valor(valor: float) -> str:
    """
    Classifica venda por faixa de valor.

    Útil para análises de BI sem necessidade de cálculos complexos.
    """
    if pd.isna(valor) or valor is None:
        return 'DESCONHECIDO'

    for min_val, max_val, categoria in CATEGORIAS_VALOR:
        if min_val <= valor < max_val:
            return categoria

    return 'DESCONHECIDO'


def calcular_margem(valor_total: float, custo: float) -> Optional[float]:
    """
    Calcula margem de lucro em percentual.

    margem = (valor_total - custo) / valor_total * 100
    """
    if pd.isna(valor_total) or pd.isna(custo) or valor_total == 0:
        return None

    try:
        return round((valor_total - custo) / valor_total * 100, 2)
    except (ZeroDivisionError, TypeError):
        return None


def extract_vendas(s3: S3Local, input_prefix: str) -> pd.DataFrame:
    """
    Lê arquivos CSV de vendas do S3 raw/.

    No Glue Spark:
        df = spark.read.option("header", "true")
                       .option("delimiter", ",")
                       .option("inferSchema", "true")
                       .csv("s3://bucket/raw/vendas/")
    """
    logger.info(f"EXTRACT VENDAS: Lendo arquivos de {input_prefix}")

    objetos = s3.list_objects(prefix=input_prefix)
    arquivos_csv = [obj for obj in objetos if obj['Key'].endswith('.csv')]

    if not arquivos_csv:
        logger.warning("Nenhum arquivo CSV de vendas encontrado!")
        return pd.DataFrame()

    logger.info(f"  {len(arquivos_csv)} arquivo(s) CSV encontrado(s)")

    dfs = []
    for obj in arquivos_csv:
        response = s3.get_object(obj['Key'])
        conteudo = response['Body'].decode('utf-8')

        try:
            df = pd.read_csv(io.StringIO(conteudo), sep=',', encoding='utf-8')
            df['_source_file'] = obj['Key']
            df['_ingestion_timestamp'] = datetime.now().isoformat()
            dfs.append(df)
            logger.info(f"  Lido: {obj['Key']} ({len(df)} registros)")
        except Exception as e:
            logger.error(f"  ERRO ao ler {obj['Key']}: {e}")

    if not dfs:
        return pd.DataFrame()

    df_total = pd.concat(dfs, ignore_index=True)
    logger.info(f"  TOTAL EXTRAÍDO: {len(df_total)} vendas")
    return df_total


def extract_clientes_processed(s3: S3Local, input_prefix: str) -> pd.DataFrame:
    """
    Lê clientes já processados do S3 processed/ (formato Parquet).

    No Glue Spark:
        df_clientes = spark.read.parquet("s3://bucket/processed/clientes/")

    Usado para enriquecer as vendas com dados do cliente (JOIN).
    """
    logger.info(f"EXTRACT CLIENTES: Lendo Parquet de {input_prefix}")

    objetos = s3.list_objects(prefix=input_prefix)
    arquivos_parquet = [obj for obj in objetos if obj['Key'].endswith('.parquet')]

    if not arquivos_parquet:
        logger.warning("Nenhum Parquet de clientes encontrado. Prosseguindo sem enriquecimento.")
        return pd.DataFrame()

    dfs = []
    for obj in arquivos_parquet:
        response = s3.get_object(obj['Key'])
        buffer = io.BytesIO(response['Body'])
        df = pd.read_parquet(buffer)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df_clientes = pd.concat(dfs, ignore_index=True)

    # Seleciona apenas colunas relevantes para o JOIN
    colunas_join = ['cpf', 'nome', 'email', 'cidade', 'uf', 'segmento_cliente']
    colunas_disponiveis = [c for c in colunas_join if c in df_clientes.columns]
    df_clientes = df_clientes[colunas_disponiveis].drop_duplicates(subset=['cpf'])

    logger.info(f"  {len(df_clientes)} clientes carregados para enriquecimento")
    return df_clientes


def transform_vendas(df_vendas: pd.DataFrame,
                     df_clientes: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformações nas vendas e enriquece com dados do cliente.

    No Glue Spark com JOIN:
        df_enriquecido = df_vendas.join(
            df_clientes.select('cpf', 'nome', 'uf', 'segmento_cliente'),
            on='cpf',
            how='left'
        )
    """
    logger.info("TRANSFORM VENDAS: Aplicando transformações...")
    total_inicial = len(df_vendas)

    # --- Passo 1: Remover linhas vazias ---
    df_vendas = df_vendas.dropna(how='all')

    # --- Passo 2: Limpar valores monetários ---
    for col_monetaria in ['valor_total', 'valor_produto', 'custo', 'desconto']:
        if col_monetaria in df_vendas.columns:
            df_vendas[col_monetaria] = df_vendas[col_monetaria].apply(
                limpar_valor_monetario
            )

    # --- Passo 3: Remover vendas com valor inválido ---
    if 'valor_total' in df_vendas.columns:
        invalidos = df_vendas['valor_total'].isna().sum()
        df_vendas = df_vendas.dropna(subset=['valor_total'])
        df_vendas = df_vendas[df_vendas['valor_total'] > 0]
        logger.info(f"  Vendas com valor inválido removidas: {invalidos}")

    # --- Passo 4: Normalizar datas ---
    for col_data in ['data_venda', 'data_entrega', 'data_cancelamento']:
        if col_data in df_vendas.columns:
            df_vendas[col_data] = pd.to_datetime(
                df_vendas[col_data], errors='coerce'
            )

    # --- Passo 5: Campos derivados ---
    if 'valor_total' in df_vendas.columns:
        # Categoria por faixa de valor
        df_vendas['categoria_valor'] = df_vendas['valor_total'].apply(
            categorizar_valor
        )

    if 'valor_total' in df_vendas.columns and 'custo' in df_vendas.columns:
        # Margem de lucro
        df_vendas['margem_pct'] = df_vendas.apply(
            lambda row: calcular_margem(row['valor_total'], row['custo']),
            axis=1
        )

    # --- Passo 6: Extrair campos de data para particionamento ---
    if 'data_venda' in df_vendas.columns:
        df_vendas['ano_venda'] = df_vendas['data_venda'].dt.year.astype('Int64')
        df_vendas['mes_venda'] = df_vendas['data_venda'].dt.month.astype('Int64')
        df_vendas['dia_semana'] = df_vendas['data_venda'].dt.day_name()

    # --- Passo 7: JOIN com clientes ---
    # Garante que a chave de join é string em ambos os lados
    if not df_clientes.empty and 'cpf_cliente' in df_vendas.columns:
        df_vendas['cpf_cliente'] = df_vendas['cpf_cliente'].astype(str).str.replace(r'\D', '', regex=True)
        df_clientes['cpf'] = df_clientes['cpf'].astype(str).str.replace(r'\D', '', regex=True)
        df_vendas = df_vendas.merge(
            df_clientes.rename(columns={'cpf': 'cpf_cliente'}),
            on='cpf_cliente',
            how='left'
        )
        logger.info("  JOIN com clientes aplicado")
    elif not df_clientes.empty and 'cpf' in df_vendas.columns:
        df_vendas['cpf'] = df_vendas['cpf'].astype(str).str.replace(r'\D', '', regex=True)
        df_clientes['cpf'] = df_clientes['cpf'].astype(str).str.replace(r'\D', '', regex=True)
        df_vendas = df_vendas.merge(
            df_clientes,
            on='cpf',
            how='left'
        )
        logger.info("  JOIN com clientes aplicado")

    # --- Passo 8: Limpar CPF do cliente (remove formatação) ---
    for col_cpf in ['cpf', 'cpf_cliente']:
        if col_cpf in df_vendas.columns:
            df_vendas[col_cpf] = df_vendas[col_cpf].astype(str).str.replace(
                r'\D', '', regex=True
            )

    # --- Passo 9: Metadados de auditoria ---
    df_vendas['_processed_at'] = datetime.now().isoformat()
    df_vendas['_job_name'] = 'vendas_raw_to_processed'

    total_final = len(df_vendas)
    logger.info(f"  TOTAL após transformações: {total_final} "
                f"(removidos: {total_inicial - total_final})")

    return df_vendas


def load_vendas_to_processed(df: pd.DataFrame, s3: S3Local,
                              output_prefix: str) -> List[str]:
    """
    Salva vendas processadas como Parquet, particionado por ano/mês.

    Particionamento por data é FUNDAMENTAL para performance:
        processed/vendas/ano=2024/mes=01/vendas_20240115.parquet
        processed/vendas/ano=2024/mes=02/vendas_20240215.parquet

    Com isso, uma query "SELECT * WHERE mes = '2024-01'" lê apenas 1 arquivo.
    Sem isso, leria TODOS os arquivos (table scan completo).
    """
    logger.info(f"LOAD VENDAS: Salvando para {output_prefix}")

    arquivos_criados = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Particiona por ano e mês de venda
    if 'ano_venda' in df.columns and 'mes_venda' in df.columns:
        # Agrupa por ano e mês
        df_com_part = df.copy()
        df_com_part['_particao'] = (
            'ano=' + df_com_part['ano_venda'].astype(str) +
            '/mes=' + df_com_part['mes_venda'].astype(str).str.zfill(2)
        )
        grupos = df_com_part.groupby('_particao')
    else:
        df_com_part = df.copy()
        df_com_part['_particao'] = 'sem_data'
        grupos = df_com_part.groupby('_particao')

    for particao, grupo_df in grupos:
        # Remove coluna auxiliar de partição
        grupo_df = grupo_df.drop(columns=['_particao'])

        key = f"{output_prefix}{particao}/vendas_{timestamp}.parquet"

        table = pa.Table.from_pandas(grupo_df, preserve_index=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        parquet_bytes = buffer.getvalue()

        s3.put_object(
            key,
            parquet_bytes,
            metadata={
                'job': 'vendas_raw_to_processed',
                'particao': str(particao),
                'rows': str(len(grupo_df))
            }
        )
        logger.info(f"  Salvo: {key} ({len(grupo_df)} registros)")
        arquivos_criados.append(key)

    return arquivos_criados


def run_job() -> Dict:
    """Executa o job completo de ETL para vendas."""
    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("INICIANDO JOB: vendas_raw_to_processed")
    logger.info("=" * 60)

    s3 = S3Local()
    resultado = {
        'job_name': 'vendas_raw_to_processed',
        'status': 'RUNNING',
        'registros_lidos': 0,
        'registros_processados': 0,
        'arquivos_criados': [],
        'erros': []
    }

    try:
        # Extrai vendas brutas
        df_vendas = extract_vendas(s3, JOB_CONFIG['input_prefix_vendas'])

        if df_vendas.empty:
            logger.warning("Nenhum dado de vendas encontrado!")
            resultado['status'] = 'SUCCESS_NO_DATA'
            return resultado

        resultado['registros_lidos'] = len(df_vendas)

        # Carrega clientes para enriquecimento (JOIN)
        df_clientes = extract_clientes_processed(
            s3, JOB_CONFIG['input_prefix_clientes']
        )

        # Transforma
        df_processado = transform_vendas(df_vendas, df_clientes)
        resultado['registros_processados'] = len(df_processado)

        # Carrega
        arquivos = load_vendas_to_processed(
            df_processado, s3, JOB_CONFIG['output_prefix']
        )
        resultado['arquivos_criados'] = arquivos

        duracao = (datetime.now() - inicio).total_seconds()
        resultado['status'] = 'SUCCESS'
        resultado['duracao_segundos'] = duracao

        logger.info(f"\nJOB CONCLUÍDO: {len(df_processado)} vendas processadas em {duracao:.2f}s")

    except Exception as e:
        logger.error(f"ERRO NO JOB: {e}", exc_info=True)
        resultado['status'] = 'FAILED'
        resultado['erros'].append(str(e))

    return resultado


if __name__ == '__main__':
    resultado = run_job()
    sys.exit(0 if resultado['status'] in ('SUCCESS', 'SUCCESS_NO_DATA') else 1)

    # ==========================================================================
    # VISUALIZAÇÃO — descomente o bloco abaixo para ver os dados processados
    # Basta remover o  '''  de abertura e fechamento
    # ==========================================================================
    '''
    import glob
    import pandas as pd
    from tabulate import tabulate

    print("\n" + "=" * 60)
    print("PRÉVIA — s3/processed/vendas/ (Parquet após ETL)")
    print("=" * 60)

    arquivos = sorted(glob.glob("s3/processed/vendas/**/*.parquet", recursive=True))
    if not arquivos:
        print("  Nenhum Parquet encontrado. Rode o job primeiro.")
    else:
        dfs = [pd.read_parquet(f) for f in arquivos]
        df = pd.concat(dfs, ignore_index=True)

        print(f"\n  Total de registros : {len(df)}")
        print(f"  Arquivos Parquet   : {len(arquivos)} (particionado por ano/mês)\n")

        # Amostra de dados
        colunas = ["id_pedido", "produto_nome", "valor_total", "desconto",
                   "categoria_valor", "margem_pct", "canal_venda",
                   "forma_pagamento", "status_venda", "nome", "uf"]
        colunas = [c for c in colunas if c in df.columns]
        print(tabulate(df[colunas].head(10), headers="keys",
                       tablefmt="rounded_outline", showindex=False,
                       floatfmt=".2f"))
        print(f"  ... e mais {max(0, len(df)-10)} linhas")

        # Campos calculados pelo ETL (não existiam no raw)
        print("\n  Campos calculados pelo ETL (novos em relação ao raw):")
        novos = ["categoria_valor", "margem_pct", "ano_venda", "mes_venda", "dia_semana"]
        novos = [c for c in novos if c in df.columns]
        print(tabulate(df[novos].head(8), headers="keys",
                       tablefmt="simple", showindex=False, floatfmt=".2f"))

        # Receita por canal
        print("\n  Receita por canal de venda:")
        canal = df.groupby("canal_venda")["valor_total"].agg(
            Qtd="count", Receita="sum", Ticket_Medio="mean"
        ).round(2).reset_index()
        print(tabulate(canal, headers="keys", tablefmt="simple",
                       showindex=False, floatfmt=".2f"))

        # Receita por mês
        if "ano_venda" in df.columns:
            print("\n  Receita por mês:")
            serie = df.groupby(["ano_venda", "mes_venda"])["valor_total"].agg(
                Qtd="count", Receita="sum"
            ).round(2).reset_index().sort_values(["ano_venda", "mes_venda"])
            print(tabulate(serie, headers="keys", tablefmt="simple",
                           showindex=False, floatfmt=".2f"))

        # Verificação do JOIN com clientes
        if "nome" in df.columns:
            com_nome = df["nome"].notna().sum()
            sem_nome = df["nome"].isna().sum()
            print(f"\n  JOIN com clientes:")
            print(f"    Com nome do cliente : {com_nome} ({com_nome/len(df)*100:.1f}%)")
            print(f"    Sem match (nome nulo): {sem_nome}")

        # Resumo financeiro
        print(f"\n  Resumo financeiro:")
        print(f"    Receita total   : R$ {df['valor_total'].sum():,.2f}")
        print(f"    Ticket médio    : R$ {df['valor_total'].mean():,.2f}")
        print(f"    Maior venda     : R$ {df['valor_total'].max():,.2f}")
        if "desconto" in df.columns:
            print(f"    Total descontos : R$ {df['desconto'].sum():,.2f}")
        if "margem_pct" in df.columns:
            print(f"    Margem média    : {df['margem_pct'].mean():.1f}%")
    '''
    sys.exit(0 if resultado['status'] in ('SUCCESS', 'SUCCESS_NO_DATA') else 1)
