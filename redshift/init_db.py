"""
init_db.py - Inicializa o banco de dados PostgreSQL (simulação do Redshift)

Este script:
1. Conecta ao PostgreSQL local
2. Executa todos os arquivos DDL (cria schemas, tabelas, stored procedures)
3. Verifica se o banco foi criado corretamente

No AWS real: o Redshift seria provisionado via CloudFormation ou Terraform.
Aqui, executamos os scripts SQL manualmente.

USO:
    python redshift/init_db.py
    python redshift/init_db.py --skip-data   # Pula carga de dados de exemplo
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [INIT-DB] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('InitDB')

projeto_root = Path(__file__).parent.parent

# Configurações do banco (lidas do .env ou variáveis de ambiente)
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'dataplatform'),
    'user': os.getenv('POSTGRES_USER', 'dataplatform_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'dataplatform_pass123'),
}

# Ordem dos scripts DDL (importante manter a sequência)
DDL_SCRIPTS = [
    'redshift/ddl/001_create_schemas.sql',
    'redshift/ddl/002_staging_tables.sql',
    'redshift/ddl/003_dw_tables.sql',
    'redshift/ddl/004_data_marts.sql',
    'redshift/stored_procedures/sp_load_staging.sql',
    'redshift/stored_procedures/sp_transform_dw.sql',
]


def criar_banco_se_nao_existe():
    """
    Cria o banco de dados se não existir.
    PostgreSQL não suporta CREATE DATABASE IF NOT EXISTS como MySQL.
    Precisamos conectar no banco 'postgres' primeiro.
    """
    logger.info("Verificando se o banco de dados existe...")

    # Conecta no banco padrão 'postgres' para criar o banco principal
    config_postgres = DB_CONFIG.copy()
    config_postgres['database'] = 'postgres'

    try:
        conn = psycopg2.connect(**config_postgres)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Verifica se o banco já existe
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB_CONFIG['database'],)
        )

        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
            logger.info(f"Banco de dados '{DB_CONFIG['database']}' criado!")
        else:
            logger.info(f"Banco de dados '{DB_CONFIG['database']}' já existe.")

        cur.close()
        conn.close()

    except psycopg2.OperationalError as e:
        logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
        logger.error("Verifique se o Docker está rodando: docker-compose up -d postgres")
        raise


def executar_script_sql(conn, script_path: str) -> bool:
    """
    Executa um arquivo SQL no banco de dados.

    Args:
        conn: Conexão psycopg2
        script_path: Caminho para o arquivo .sql

    Returns:
        True se executou com sucesso
    """
    full_path = projeto_root / script_path

    if not full_path.exists():
        logger.error(f"Script não encontrado: {full_path}")
        return False

    logger.info(f"Executando: {script_path}")

    with open(full_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info(f"  OK: {script_path}")
        return True

    except Exception as e:
        conn.rollback()
        logger.error(f"  ERRO em {script_path}: {e}")
        return False


def verificar_estrutura(conn) -> dict:
    """
    Verifica se a estrutura foi criada corretamente.

    Returns:
        Dict com resultado da verificação
    """
    logger.info("Verificando estrutura do banco...")
    resultado = {'schemas': {}, 'tabelas': {}, 'ok': True}

    with conn.cursor() as cur:
        # Verifica schemas
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name IN ('staging', 'dw', 'dm')
            ORDER BY schema_name
        """)
        schemas = [row[0] for row in cur.fetchall()]

        for schema in ['staging', 'dw', 'dm']:
            existe = schema in schemas
            resultado['schemas'][schema] = existe
            status = "OK" if existe else "FALTANDO"
            logger.info(f"  Schema {schema}: {status}")
            if not existe:
                resultado['ok'] = False

        # Verifica tabelas principais
        tabelas_esperadas = [
            ('staging', 'stg_clientes'),
            ('staging', 'stg_vendas'),
            ('dw', 'dim_clientes'),
            ('dw', 'dim_produto'),
            ('dw', 'dim_tempo'),
            ('dw', 'fato_vendas'),
            ('dm', 'dm_vendas_diarias'),
            ('dm', 'dm_top_clientes'),
        ]

        for schema, tabela in tabelas_esperadas:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema, tabela))
            existe = cur.fetchone()[0]
            chave = f"{schema}.{tabela}"
            resultado['tabelas'][chave] = existe
            status = "OK" if existe else "FALTANDO"
            logger.info(f"  Tabela {chave}: {status}")
            if not existe:
                resultado['ok'] = False

        # Verifica dim_tempo populada
        cur.execute("SELECT COUNT(*) FROM dw.dim_tempo")
        qtd_tempo = cur.fetchone()[0]
        resultado['dim_tempo_rows'] = qtd_tempo
        logger.info(f"  dim_tempo: {qtd_tempo} datas (2022-2026)")

    return resultado


def main():
    parser = argparse.ArgumentParser(
        description='Inicializa o banco de dados PostgreSQL (simula Redshift)'
    )
    parser.add_argument(
        '--skip-data',
        action='store_true',
        help='Pula carga de dados de exemplo'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Força recriação mesmo se já existir'
    )
    args = parser.parse_args()

    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("INICIALIZANDO BANCO DE DADOS")
    logger.info(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    logger.info(f"Database: {DB_CONFIG['database']}")
    logger.info("=" * 60)

    # Cria o banco se necessário
    try:
        criar_banco_se_nao_existe()
    except Exception as e:
        logger.error("Falha ao criar banco. Certifique-se que o Docker está rodando.")
        sys.exit(1)

    # Conecta ao banco principal
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Conectado ao PostgreSQL com sucesso!")
    except psycopg2.OperationalError as e:
        logger.error(f"Falha na conexão: {e}")
        sys.exit(1)

    # Executa scripts DDL
    erros = 0
    for script in DDL_SCRIPTS:
        if not executar_script_sql(conn, script):
            erros += 1

    if erros > 0:
        logger.error(f"ATENÇÃO: {erros} script(s) falharam!")
    else:
        logger.info("Todos os scripts DDL executados com sucesso!")

    # Verifica estrutura
    resultado = verificar_estrutura(conn)

    conn.close()

    duracao = (datetime.now() - inicio).total_seconds()

    logger.info("\n" + "=" * 60)
    if resultado['ok']:
        logger.info("BANCO DE DADOS INICIALIZADO COM SUCESSO!")
        logger.info(f"Schemas: staging, dw, dm")
        logger.info(f"Tabelas: {len(resultado['tabelas'])} criadas")
        logger.info(f"dim_tempo: {resultado.get('dim_tempo_rows', 0)} datas")
        logger.info(f"Duração: {duracao:.1f}s")
        logger.info("")
        logger.info("Próximos passos:")
        logger.info("  1. Gerar dados de exemplo:")
        logger.info("     python scripts/generate_sample_data.py")
        logger.info("  2. Executar pipeline completo:")
        logger.info("     ./scripts/run_full_pipeline.sh")
        logger.info("  3. Acessar pgAdmin: http://localhost:5050")
    else:
        logger.error("FALHA na verificação do banco!")
    logger.info("=" * 60)

    sys.exit(0 if resultado['ok'] else 1)


if __name__ == '__main__':
    main()
