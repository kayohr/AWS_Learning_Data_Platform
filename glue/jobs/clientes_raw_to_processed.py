"""
clientes_raw_to_processed.py - Job Glue: Transforma clientes de raw para processed

CONCEITO AWS GLUE:
    No AWS Glue real, este script seria um "Glue ETL Job" rodando em Apache Spark.
    Você criaria via console:
        AWS Console → Glue → ETL Jobs → Create Job
        Tipo: "Spark" ou "Python Shell"
        Script: este arquivo
        IAM Role: glue-etl-role (acesso ao S3)
        Workers: G.1X (4 vCPUs, 16GB), quantidade: 10

    O Glue gerenciaria automaticamente:
        - Cluster Spark (subir e descer máquinas)
        - Retry automático em caso de falha
        - Logs no CloudWatch
        - Checkpoint em S3

    Aqui usamos Pandas (sem Spark), mas a lógica ETL é idêntica.

PIPELINE:
    INPUT:  s3/raw/clientes/**/*.json      (dados brutos do CRM)
    OUTPUT: s3/processed/clientes/         (Parquet, limpo, deduplificado)

    Transformações:
        1. Lê todos os JSONs da pasta raw/clientes/
        2. Valida e limpa CPF (remove formatação)
        3. Normaliza nomes (title case)
        4. Valida emails (formato básico)
        5. Padroniza cidades (lista de cidades válidas)
        6. Remove duplicatas por CPF
        7. Salva como Parquet em processed/clientes/
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

# Adiciona o diretório raiz do projeto ao path
projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

from s3.scripts.s3_utils import S3Local

# Configuração de logging
# No AWS Glue real: logs vão automaticamente para CloudWatch
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [GLUE-JOB] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('GlueJob-Clientes')


# =============================================================================
# CONFIGURAÇÕES DO JOB
# No AWS Glue, essas seriam "Job Parameters" configuráveis via console
# =============================================================================
JOB_CONFIG = {
    'input_prefix': 'raw/clientes/',
    'output_prefix': 'processed/clientes/',
    'output_format': 'parquet',           # Formato de saída
    'compression': 'snappy',              # Compressão Parquet (snappy é balanceado)
    'partition_columns': ['uf'],          # Colunas de particionamento no S3
    'dedup_key': 'cpf',                   # Chave para remoção de duplicatas
    'max_rows_per_file': 100000,          # Tamanho máximo por arquivo Parquet
}

# Lista de UFs válidas do Brasil
UFS_VALIDAS = {
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
    'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
    'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
}


# =============================================================================
# FUNÇÕES DE VALIDAÇÃO E LIMPEZA
# =============================================================================

def limpar_cpf(cpf: str) -> Optional[str]:
    """
    Remove formatação do CPF e valida o formato básico.

    CPF no Brasil: 000.000.000-00 (11 dígitos)

    No AWS Glue com Spark seria:
        from pyspark.sql.functions import regexp_replace, length
        df = df.withColumn('cpf', regexp_replace('cpf', r'[.\-]', ''))
        df = df.filter(length('cpf') == 11)

    Args:
        cpf: CPF com ou sem formatação

    Returns:
        CPF apenas com números, ou None se inválido
    """
    if not cpf or not isinstance(cpf, str):
        return None

    # Remove tudo que não é dígito
    cpf_limpo = re.sub(r'\D', '', cpf)

    # CPF deve ter 11 dígitos
    if len(cpf_limpo) != 11:
        return None

    # Verifica se todos os dígitos são iguais (CPF inválido como 111.111.111-11)
    if len(set(cpf_limpo)) == 1:
        return None

    return cpf_limpo


def validar_email(email: str) -> bool:
    """
    Valida formato básico de e-mail.

    No Glue Spark: df.filter(col('email').rlike(r'^[^@]+@[^@]+\.[^@]+$'))

    Args:
        email: String com o e-mail

    Returns:
        True se o formato parece válido
    """
    if not email or not isinstance(email, str):
        return False

    padrao = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(padrao, email.strip()))


def normalizar_nome(nome: str) -> Optional[str]:
    """
    Normaliza o nome: Title Case, remove espaços extras.

    Exemplos:
        'JOÃO DA SILVA' → 'João da Silva'
        'maria santos  ' → 'Maria Santos'
        'PEDRO DE OLIVEIRA' → 'Pedro de Oliveira'

    No Glue Spark:
        from pyspark.sql.functions import initcap, trim
        df = df.withColumn('nome', initcap(trim('nome')))
    """
    if not nome or not isinstance(nome, str):
        return None

    # Remove espaços extras
    nome_limpo = ' '.join(nome.split())

    # Preposições que ficam em minúsculo (exceto no início)
    preposicoes = {'da', 'de', 'do', 'das', 'dos', 'e', 'em'}

    partes = nome_limpo.lower().split()
    resultado = []

    for i, parte in enumerate(partes):
        if i == 0 or parte not in preposicoes:
            resultado.append(parte.capitalize())
        else:
            resultado.append(parte)

    return ' '.join(resultado)


def normalizar_telefone(telefone: str) -> Optional[str]:
    """
    Normaliza telefone brasileiro para formato padrão: (XX) XXXXX-XXXX

    Args:
        telefone: Telefone em qualquer formato

    Returns:
        Telefone formatado ou None se inválido
    """
    if not telefone or not isinstance(telefone, str):
        return None

    # Remove tudo que não é dígito
    digitos = re.sub(r'\D', '', telefone)

    # Telefone celular: 11 dígitos com DDD
    if len(digitos) == 11:
        return f"({digitos[:2]}) {digitos[2:7]}-{digitos[7:]}"
    # Telefone fixo: 10 dígitos com DDD
    elif len(digitos) == 10:
        return f"({digitos[:2]}) {digitos[2:6]}-{digitos[6:]}"

    return None


def normalizar_uf(uf: str) -> Optional[str]:
    """
    Valida e normaliza UF (estado brasileiro).

    Args:
        uf: Sigla do estado

    Returns:
        UF em maiúsculas se válida, None caso contrário
    """
    if not uf or not isinstance(uf, str):
        return None

    uf_upper = uf.strip().upper()
    if uf_upper in UFS_VALIDAS:
        return uf_upper

    return None


# =============================================================================
# EXTRAÇÃO (E do ETL)
# =============================================================================

def extract_from_raw(s3: S3Local, input_prefix: str) -> pd.DataFrame:
    """
    Lê todos os arquivos JSON da zona raw/clientes/.

    No AWS Glue Spark, seria:
        datasource = glueContext.create_dynamic_frame.from_catalog(
            database="raw_db",
            table_name="clientes"
        )
        df = datasource.toDF()

    Localmente, lemos todos os arquivos JSON e concatenamos.

    Args:
        s3: Cliente S3 local
        input_prefix: Prefixo dos arquivos de entrada

    Returns:
        DataFrame com todos os clientes lidos
    """
    logger.info(f"EXTRACT: Lendo arquivos de {input_prefix}")

    # Lista todos os arquivos JSON na pasta
    objetos = s3.list_objects(prefix=input_prefix)
    arquivos_json = [obj for obj in objetos if obj['Key'].endswith('.json')]

    if not arquivos_json:
        logger.warning("Nenhum arquivo JSON encontrado!")
        return pd.DataFrame()

    logger.info(f"  {len(arquivos_json)} arquivo(s) encontrado(s)")

    dfs = []
    for obj in arquivos_json:
        # Lê o arquivo JSON
        response = s3.get_object(obj['Key'])
        conteudo = response['Body'].decode('utf-8')

        try:
            dados = json.loads(conteudo)

            # Suporta tanto lista de registros quanto registro único
            if isinstance(dados, list):
                df = pd.DataFrame(dados)
            elif isinstance(dados, dict):
                # Verifica se tem chave 'clientes' ou 'data'
                if 'clientes' in dados:
                    df = pd.DataFrame(dados['clientes'])
                elif 'data' in dados:
                    df = pd.DataFrame(dados['data'])
                else:
                    df = pd.DataFrame([dados])
            else:
                logger.warning(f"  Formato inesperado em {obj['Key']}, pulando...")
                continue

            # Adiciona metadados de origem
            df['_source_file'] = obj['Key']
            df['_ingestion_timestamp'] = datetime.now().isoformat()

            dfs.append(df)
            logger.info(f"  Lido: {obj['Key']} ({len(df)} registros)")

        except json.JSONDecodeError as e:
            logger.error(f"  ERRO ao ler {obj['Key']}: {e}")

    if not dfs:
        return pd.DataFrame()

    # Concatena todos os DataFrames
    df_total = pd.concat(dfs, ignore_index=True)
    logger.info(f"  TOTAL EXTRAÍDO: {len(df_total)} registros")

    return df_total


# =============================================================================
# TRANSFORMAÇÃO (T do ETL)
# =============================================================================

def transform_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas as transformações nos dados de clientes.

    No AWS Glue Spark, cada transformação seria uma operação no DynamicFrame:
        from awsglue.transforms import *
        mapped_df = ApplyMapping.apply(
            frame=datasource,
            mappings=[("id_cliente", "long", "id", "long"), ...]
        )

    Aqui fazemos o equivalente com Pandas.

    Args:
        df: DataFrame bruto lido do S3 raw/

    Returns:
        DataFrame transformado, limpo e validado
    """
    logger.info("TRANSFORM: Aplicando transformações...")
    total_inicial = len(df)

    # --- PASSO 1: Remover linhas completamente vazias ---
    df = df.dropna(how='all')
    logger.info(f"  Após remover linhas vazias: {len(df)} registros")

    # --- PASSO 2: Limpar e validar CPF ---
    if 'cpf' in df.columns:
        df['cpf'] = df['cpf'].astype(str).apply(limpar_cpf)
        # Remove registros com CPF inválido
        invalidos_cpf = df['cpf'].isna().sum()
        df = df.dropna(subset=['cpf'])
        logger.info(f"  CPFs inválidos removidos: {invalidos_cpf}")

    # --- PASSO 3: Normalizar nomes ---
    if 'nome' in df.columns:
        df['nome'] = df['nome'].astype(str).apply(normalizar_nome)

    # --- PASSO 4: Validar e-mails ---
    if 'email' in df.columns:
        mascara_email_valido = df['email'].apply(validar_email)
        df.loc[~mascara_email_valido, 'email'] = None  # Nullifica emails inválidos
        emails_invalidos = (~mascara_email_valido).sum()
        if emails_invalidos > 0:
            logger.info(f"  E-mails inválidos nullificados: {emails_invalidos}")

    # --- PASSO 5: Normalizar telefone ---
    if 'telefone' in df.columns:
        df['telefone'] = df['telefone'].astype(str).apply(normalizar_telefone)

    # --- PASSO 6: Normalizar UF ---
    if 'uf' in df.columns:
        df['uf'] = df['uf'].astype(str).apply(normalizar_uf)
    elif 'estado' in df.columns:
        df['uf'] = df['estado'].astype(str).apply(normalizar_uf)

    # --- PASSO 7: Padronizar tipos de dados ---
    if 'id' in df.columns:
        df['id'] = pd.to_numeric(df['id'], errors='coerce')

    if 'data_nascimento' in df.columns:
        df['data_nascimento'] = pd.to_datetime(
            df['data_nascimento'], errors='coerce', format='%Y-%m-%d'
        )

    if 'data_cadastro' in df.columns:
        df['data_cadastro'] = pd.to_datetime(
            df['data_cadastro'], errors='coerce'
        )

    # --- PASSO 8: Remover duplicatas por CPF ---
    # Mantém o registro mais recente (maior data_cadastro ou último lido)
    if 'cpf' in df.columns:
        total_antes_dedup = len(df)
        if 'data_cadastro' in df.columns:
            df = df.sort_values('data_cadastro', ascending=False)

        df = df.drop_duplicates(subset=['cpf'], keep='first')
        duplicatas_removidas = total_antes_dedup - len(df)
        if duplicatas_removidas > 0:
            logger.info(f"  Duplicatas por CPF removidas: {duplicatas_removidas}")

    # --- PASSO 9: Adicionar colunas de auditoria ---
    # No AWS Glue, isso seria feito via ApplyMapping ou Map transform
    df['_processed_at'] = datetime.now().isoformat()
    df['_job_name'] = 'clientes_raw_to_processed'

    # --- PASSO 10: Ordenar colunas ---
    # Coloca colunas de auditoria (_*) no final
    colunas_dados = [c for c in df.columns if not c.startswith('_')]
    colunas_meta = [c for c in df.columns if c.startswith('_')]
    df = df[colunas_dados + colunas_meta]

    total_final = len(df)
    logger.info(f"  TOTAL após transformações: {total_final} registros "
                f"(removidos: {total_inicial - total_final})")

    return df


# =============================================================================
# CARGA (L do ETL)
# =============================================================================

def load_to_processed(df: pd.DataFrame, s3: S3Local,
                      output_prefix: str, partition_col: str = 'uf') -> List[str]:
    """
    Salva o DataFrame transformado como Parquet no S3 processed/.

    Por que Parquet?
        - Formato colunar: queries analíticas leem só as colunas necessárias
        - Compressão: ~10x menor que CSV/JSON
        - Schema embutido: não precisa definir tipos manualmente
        - Nativo em Spark, Glue, Athena, Redshift Spectrum

    No AWS Glue Spark:
        glueContext.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="s3",
            connection_options={
                "path": "s3://bucket/processed/clientes/",
                "partitionKeys": ["uf"]
            },
            format="glueparquet",
            format_options={"compression": "snappy"}
        )

    Args:
        df: DataFrame transformado
        s3: Cliente S3 local
        output_prefix: Prefixo de destino
        partition_col: Coluna para particionar

    Returns:
        Lista de keys dos arquivos criados
    """
    logger.info(f"LOAD: Salvando para {output_prefix}")

    arquivos_criados = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Particiona por UF (estado) para otimizar queries futuras
    # No S3 real: processed/clientes/uf=SP/clientes_20240115.parquet
    if partition_col in df.columns and df[partition_col].notna().any():
        grupos = df.groupby(partition_col, dropna=False)
    else:
        # Sem particionamento, salva tudo junto
        grupos = [('all', df)]

    for valor_particao, grupo_df in grupos:
        if grupo_df.empty:
            continue

        # Define o caminho com particionamento (estilo Hive)
        if partition_col in df.columns and valor_particao != 'all':
            key = f"{output_prefix}{partition_col}={valor_particao}/clientes_{timestamp}.parquet"
        else:
            key = f"{output_prefix}clientes_{timestamp}.parquet"

        # Converte para Parquet usando PyArrow
        table = pa.Table.from_pandas(grupo_df, preserve_index=False)

        # Salva o Parquet
        import io
        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression='snappy',  # Compressão balanceada velocidade/tamanho
        )
        parquet_bytes = buffer.getvalue()

        # Faz o "upload" para o S3 local
        s3.put_object(
            key,
            parquet_bytes,
            metadata={
                'job': 'clientes_raw_to_processed',
                'particao': str(valor_particao),
                'rows': str(len(grupo_df))
            }
        )

        logger.info(f"  Salvo: {key} ({len(grupo_df)} registros, "
                    f"{len(parquet_bytes)/1024:.1f} KB)")
        arquivos_criados.append(key)

    return arquivos_criados


# =============================================================================
# FUNÇÃO PRINCIPAL DO JOB
# =============================================================================

def run_job(data_processamento: str = None) -> Dict:
    """
    Executa o job completo de ETL para clientes.

    No AWS Glue, este seria o ponto de entrada do script:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'data_processamento'])

    Args:
        data_processamento: Data no formato YYYY-MM-DD (padrão: hoje)

    Returns:
        Dict com resultado da execução (sucesso, registros processados, etc.)
    """
    if data_processamento is None:
        data_processamento = datetime.now().strftime('%Y-%m-%d')

    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("INICIANDO JOB: clientes_raw_to_processed")
    logger.info(f"Data de processamento: {data_processamento}")
    logger.info("=" * 60)

    s3 = S3Local()
    resultado = {
        'job_name': 'clientes_raw_to_processed',
        'data_processamento': data_processamento,
        'status': 'RUNNING',
        'registros_lidos': 0,
        'registros_processados': 0,
        'arquivos_criados': [],
        'erros': []
    }

    try:
        # --- EXTRAÇÃO ---
        df_raw = extract_from_raw(s3, JOB_CONFIG['input_prefix'])

        if df_raw.empty:
            logger.warning("Nenhum dado encontrado para processar!")
            resultado['status'] = 'SUCCESS_NO_DATA'
            return resultado

        resultado['registros_lidos'] = len(df_raw)

        # --- TRANSFORMAÇÃO ---
        df_processado = transform_clientes(df_raw)
        resultado['registros_processados'] = len(df_processado)

        # --- CARGA ---
        arquivos = load_to_processed(
            df_processado,
            s3,
            JOB_CONFIG['output_prefix'],
            partition_col='uf'
        )
        resultado['arquivos_criados'] = arquivos

        duracao = (datetime.now() - inicio).total_seconds()
        resultado['status'] = 'SUCCESS'
        resultado['duracao_segundos'] = duracao

        logger.info("=" * 60)
        logger.info("JOB CONCLUÍDO COM SUCESSO!")
        logger.info(f"  Registros lidos    : {resultado['registros_lidos']}")
        logger.info(f"  Registros salvos   : {resultado['registros_processados']}")
        logger.info(f"  Arquivos criados   : {len(arquivos)}")
        logger.info(f"  Duração            : {duracao:.2f}s")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"ERRO NO JOB: {e}", exc_info=True)
        resultado['status'] = 'FAILED'
        resultado['erros'].append(str(e))
        resultado['duracao_segundos'] = (datetime.now() - inicio).total_seconds()

    return resultado


# =============================================================================
# PONTO DE ENTRADA
# =============================================================================
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Glue Job: Transforma clientes de raw para processed'
    )
    parser.add_argument(
        '--data',
        default=None,
        help='Data de processamento (YYYY-MM-DD). Padrão: hoje'
    )
    args = parser.parse_args()

    resultado = run_job(data_processamento=args.data)

    # Exibe resultado resumido
    print("\n" + "=" * 40)
    print(f"STATUS: {resultado['status']}")
    print(f"Registros processados: {resultado.get('registros_processados', 0)}")
    print("=" * 40)

    # Exit code para o Airflow saber se o job falhou
    sys.exit(0 if resultado['status'] == 'SUCCESS' else 1)

    # ==========================================================================
    # VISUALIZAÇÃO — descomente o bloco abaixo para ver os dados processados
    # Basta remover o  '''  de abertura e fechamento
    # ==========================================================================
    '''
    import glob
    import pandas as pd
    from tabulate import tabulate

    print("\n" + "=" * 60)
    print("PRÉVIA — s3/processed/clientes/ (Parquet após ETL)")
    print("=" * 60)

    arquivos = sorted(glob.glob("s3/processed/clientes/**/*.parquet", recursive=True))
    if not arquivos:
        print("  Nenhum Parquet encontrado. Rode o job primeiro.")
    else:
        # Lê todos os Parquets e junta
        dfs = [pd.read_parquet(f) for f in arquivos]
        df = pd.concat(dfs, ignore_index=True)

        print(f"\n  Total de registros : {len(df)}")
        print(f"  Arquivos Parquet   : {len(arquivos)} (particionado por UF)\n")

        # Amostra de dados
        colunas = ["cpf", "nome", "email", "telefone", "cidade", "uf",
                   "segmento_cliente", "limite_credito"]
        colunas = [c for c in colunas if c in df.columns]
        print(tabulate(df[colunas].head(10), headers="keys",
                       tablefmt="rounded_outline", showindex=False))
        print(f"  ... e mais {max(0, len(df)-10)} linhas")

        # Schema dos tipos
        print("\n  Schema (tipos de dados após ETL):")
        schema = [(col, str(df[col].dtype), df[col].isna().sum())
                  for col in df.columns if not col.startswith("_")]
        print(tabulate(schema, headers=["Coluna", "Tipo", "Nulos"],
                       tablefmt="simple"))

        # Distribuição por UF
        print("\n  Distribuição por UF (top 10):")
        dist = df["uf"].value_counts().head(10).reset_index()
        dist.columns = ["UF", "Qtd"]
        print(tabulate(dist, headers="keys", tablefmt="simple", showindex=False))

        # Distribuição por segmento
        print("\n  Por segmento:")
        seg = df["segmento_cliente"].value_counts().reset_index()
        seg.columns = ["Segmento", "Qtd"]
        print(tabulate(seg, headers="keys", tablefmt="simple", showindex=False))
    '''
