"""
visualizar_dados.py — Visualiza os dados de cada etapa do pipeline em tabelas

USO:
    python3 scripts/visualizar_dados.py              # menu interativo
    python3 scripts/visualizar_dados.py --etapa 1    # direto na etapa
    python3 scripts/visualizar_dados.py --etapa all  # todas as etapas

ETAPAS:
    1  — S3 raw/clientes     (JSON bruto)
    2  — S3 raw/vendas       (CSV bruto)
    3  — S3 processed/clientes (Parquet transformado)
    4  — S3 processed/vendas   (Parquet enriquecido)
    5  — Banco: staging (tabelas de chegada)
    6  — Banco: dw — Star Schema (dim + fato)
    7  — Banco: dm — Data Marts (views para BI)
    sql — Rodar SQL livre no banco
"""

import os
import sys
import json
import glob
import argparse
from pathlib import Path
from datetime import datetime

projeto_root = Path(__file__).parent.parent
sys.path.insert(0, str(projeto_root))

# ── dependências ──────────────────────────────────────────────────────────────
try:
    import pandas as pd
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_colwidth', 30)
    pd.set_option('display.float_format', lambda x: f'{x:,.2f}')
except ImportError:
    print("ERRO: instale pandas →  pip3 install pandas")
    sys.exit(1)

try:
    from tabulate import tabulate
    TABULATE_OK = True
except ImportError:
    TABULATE_OK = False

try:
    import pyarrow.parquet as pq
    PARQUET_OK = True
except ImportError:
    PARQUET_OK = False

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_OK = True
except ImportError:
    PSYCOPG2_OK = False

# ── cores no terminal ─────────────────────────────────────────────────────────
RESET  = '\033[0m'
VERDE  = '\033[92m'
AZUL   = '\033[94m'
AMARELO= '\033[93m'
CINZA  = '\033[90m'
NEGRITO= '\033[1m'
CIANO  = '\033[96m'
VERMELHO='\033[91m'


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def cabecalho(titulo: str, subtitulo: str = ''):
    largura = 72
    print()
    print(f"{AZUL}{NEGRITO}{'═' * largura}{RESET}")
    print(f"{AZUL}{NEGRITO}  {titulo}{RESET}")
    if subtitulo:
        print(f"{CINZA}  {subtitulo}{RESET}")
    print(f"{AZUL}{NEGRITO}{'═' * largura}{RESET}")


def secao(titulo: str):
    print(f"\n{CIANO}{NEGRITO}── {titulo} {'─' * (60 - len(titulo))}{RESET}")


def mostrar_tabela(df: pd.DataFrame, max_rows: int = 10, titulo: str = ''):
    """Exibe um DataFrame como tabela formatada no terminal."""
    if df.empty:
        print(f"  {AMARELO}(sem dados){RESET}")
        return

    if titulo:
        print(f"\n  {NEGRITO}{titulo}{RESET}")

    # Limita colunas largas
    df_display = df.copy()
    for col in df_display.select_dtypes(include='object').columns:
        df_display[col] = df_display[col].astype(str).str[:35]

    amostra = df_display.head(max_rows)

    if TABULATE_OK:
        print(tabulate(amostra, headers='keys', tablefmt='rounded_outline',
                       showindex=False, numalign='right', stralign='left'))
    else:
        print(amostra.to_string(index=False))

    total = len(df)
    if total > max_rows:
        print(f"  {CINZA}... e mais {total - max_rows} linhas (total: {total}){RESET}")
    else:
        print(f"  {CINZA}Total: {total} linha(s){RESET}")


def mostrar_resumo(df: pd.DataFrame):
    """Exibe estatísticas resumidas das colunas numéricas."""
    numericas = df.select_dtypes(include='number')
    if numericas.empty:
        return

    secao("Resumo estatístico (colunas numéricas)")
    stats = numericas.describe().round(2)
    if TABULATE_OK:
        print(tabulate(stats, headers='keys', tablefmt='rounded_outline',
                       numalign='right'))
    else:
        print(stats.to_string())


def encontrar_arquivos(padrao: str) -> list:
    """Retorna lista de arquivos que batem com o padrão glob."""
    return sorted(glob.glob(str(projeto_root / padrao), recursive=True))


def tamanho_fmt(bytes_: int) -> str:
    """Formata tamanho de arquivo de forma legível."""
    for unidade in ['B', 'KB', 'MB', 'GB']:
        if bytes_ < 1024:
            return f"{bytes_:.1f} {unidade}"
        bytes_ /= 1024
    return f"{bytes_:.1f} GB"


def conectar_banco():
    """Conecta no PostgreSQL local. Retorna conexão ou None."""
    if not PSYCOPG2_OK:
        print(f"  {VERMELHO}psycopg2 não instalado. Execute: pip3 install psycopg2-binary{RESET}")
        return None

    # Tenta ler do .env
    env_file = projeto_root / '.env'
    config = {
        'host': 'localhost',
        'port': 5432,
        'dbname': 'dataplatform',
        'user': 'dataplatform_user',
        'password': 'dataplatform_pass',
        'connect_timeout': 5
    }

    if env_file.exists():
        for linha in env_file.read_text().splitlines():
            if '=' in linha and not linha.startswith('#'):
                chave, _, valor = linha.partition('=')
                mapa = {
                    'POSTGRES_DB': 'dbname',
                    'POSTGRES_USER': 'user',
                    'POSTGRES_PASSWORD': 'password',
                    'POSTGRES_HOST': 'host',
                    'POSTGRES_PORT': 'port',
                }
                if chave.strip() in mapa:
                    config[mapa[chave.strip()]] = valor.strip().strip('"\'')

    try:
        conn = psycopg2.connect(**config)
        return conn
    except psycopg2.OperationalError as e:
        print(f"  {VERMELHO}Banco não disponível: {e}{RESET}")
        print(f"  {AMARELO}Dica: suba o PostgreSQL com: docker-compose up -d postgres{RESET}")
        return None


def rodar_query(conn, sql: str, titulo: str = '', max_rows: int = 20):
    """Executa SQL e exibe resultado como tabela."""
    try:
        df = pd.read_sql(sql, conn)
        if titulo:
            secao(titulo)
        mostrar_tabela(df, max_rows=max_rows)
        return df
    except Exception as e:
        print(f"  {VERMELHO}Erro na query: {e}{RESET}")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 1 — S3 raw/clientes (JSON bruto)
# ══════════════════════════════════════════════════════════════════════════════

def etapa1_raw_clientes():
    cabecalho(
        "ETAPA 1 — S3 raw/clientes (JSON bruto)",
        "Dados como chegam da fonte, antes de qualquer transformação"
    )

    arquivos = encontrar_arquivos('s3/raw/clientes/**/*.json')

    if not arquivos:
        print(f"\n  {AMARELO}Nenhum arquivo encontrado em s3/raw/clientes/{RESET}")
        print(f"  Execute primeiro: python3 scripts/generate_sample_data.py")
        return

    # Exibe arquivos encontrados
    secao("Arquivos no S3 raw/clientes/")
    for arq in arquivos:
        rel = Path(arq).relative_to(projeto_root)
        tam = tamanho_fmt(os.path.getsize(arq))
        print(f"  {VERDE}✓{RESET}  {rel}  {CINZA}({tam}){RESET}")

    # Lê e exibe amostra
    with open(arquivos[0], encoding='utf-8') as f:
        dados = json.load(f)

    clientes = dados.get('clientes', dados if isinstance(dados, list) else [dados])
    df = pd.DataFrame(clientes)

    secao(f"Amostra de dados — {len(df)} clientes no arquivo")
    colunas_show = ['id', 'cpf', 'nome', 'email', 'cidade', 'uf',
                    'segmento_cliente', 'limite_credito', 'ativo']
    colunas_show = [c for c in colunas_show if c in df.columns]
    mostrar_tabela(df[colunas_show])

    secao("Distribuição por UF (estado)")
    dist_uf = df['uf'].value_counts().reset_index()
    dist_uf.columns = ['UF', 'Qtd Clientes']
    dist_uf['% do Total'] = (dist_uf['Qtd Clientes'] / len(df) * 100).round(1)
    mostrar_tabela(dist_uf, max_rows=10)

    secao("Distribuição por Segmento")
    dist_seg = df['segmento_cliente'].value_counts().reset_index()
    dist_seg.columns = ['Segmento', 'Qtd']
    mostrar_tabela(dist_seg)

    secao("Informações do arquivo JSON")
    print(f"  Total de clientes : {VERDE}{dados.get('total', len(clientes))}{RESET}")
    print(f"  Gerado em         : {CINZA}{dados.get('gerado_em', 'n/d')}{RESET}")
    print(f"  Colunas           : {', '.join(df.columns.tolist())}")


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 2 — S3 raw/vendas (CSV bruto)
# ══════════════════════════════════════════════════════════════════════════════

def etapa2_raw_vendas():
    cabecalho(
        "ETAPA 2 — S3 raw/vendas (CSV bruto)",
        "Dados de vendas como chegam do ERP, sem limpeza"
    )

    arquivos = encontrar_arquivos('s3/raw/vendas/**/*.csv')

    if not arquivos:
        print(f"\n  {AMARELO}Nenhum arquivo encontrado em s3/raw/vendas/{RESET}")
        print(f"  Execute primeiro: python3 scripts/generate_sample_data.py")
        return

    secao("Arquivos no S3 raw/vendas/")
    for arq in arquivos:
        rel = Path(arq).relative_to(projeto_root)
        tam = tamanho_fmt(os.path.getsize(arq))
        print(f"  {VERDE}✓{RESET}  {rel}  {CINZA}({tam}){RESET}")

    df = pd.read_csv(arquivos[0])

    secao(f"Amostra de dados — {len(df)} vendas")
    colunas_show = ['id_pedido', 'produto_nome', 'quantidade',
                    'valor_unitario', 'desconto', 'valor_total',
                    'canal_venda', 'forma_pagamento', 'status_venda']
    colunas_show = [c for c in colunas_show if c in df.columns]
    mostrar_tabela(df[colunas_show])

    secao("Totais por canal de venda")
    if 'canal_venda' in df.columns and 'valor_total' in df.columns:
        canal = df.groupby('canal_venda').agg(
            Qtd_Vendas=('id_pedido', 'count'),
            Valor_Total=('valor_total', 'sum'),
            Ticket_Medio=('valor_total', 'mean')
        ).round(2).reset_index()
        canal.columns = ['Canal', 'Qtd Vendas', 'Valor Total (R$)', 'Ticket Médio (R$)']
        mostrar_tabela(canal)

    secao("Totais por status da venda")
    if 'status_venda' in df.columns:
        status = df.groupby('status_venda').agg(
            Qtd=('id_pedido', 'count'),
            Valor=('valor_total', 'sum')
        ).round(2).reset_index()
        status.columns = ['Status', 'Qtd', 'Valor Total (R$)']
        status['% Qtd'] = (status['Qtd'] / len(df) * 100).round(1)
        mostrar_tabela(status)

    secao("Top 5 produtos mais vendidos")
    if 'produto_nome' in df.columns:
        top = df.groupby('produto_nome').agg(
            Qtd=('quantidade', 'sum'),
            Receita=('valor_total', 'sum')
        ).round(2).sort_values('Receita', ascending=False).head(5).reset_index()
        top.columns = ['Produto', 'Unidades Vendidas', 'Receita Total (R$)']
        mostrar_tabela(top)

    secao("Resumo financeiro geral")
    if 'valor_total' in df.columns:
        print(f"  Total de vendas   : {VERDE}{len(df):,}{RESET}")
        print(f"  Receita total     : {VERDE}R$ {df['valor_total'].sum():,.2f}{RESET}")
        print(f"  Ticket médio      : {VERDE}R$ {df['valor_total'].mean():,.2f}{RESET}")
        print(f"  Maior venda       : {VERDE}R$ {df['valor_total'].max():,.2f}{RESET}")
        print(f"  Menor venda       : {VERDE}R$ {df['valor_total'].min():,.2f}{RESET}")
        if 'desconto' in df.columns:
            total_desc = df['desconto'].sum()
            print(f"  Total descontos   : {AMARELO}R$ {total_desc:,.2f}{RESET}")


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 3 — S3 processed/clientes (Parquet transformado)
# ══════════════════════════════════════════════════════════════════════════════

def etapa3_processed_clientes():
    cabecalho(
        "ETAPA 3 — S3 processed/clientes (Parquet limpo)",
        "Após ETL: CPF validado, nomes normalizados, duplicatas removidas"
    )

    if not PARQUET_OK:
        print(f"  {VERMELHO}pyarrow não instalado. Execute: pip3 install pyarrow{RESET}")
        return

    arquivos = encontrar_arquivos('s3/processed/clientes/**/*.parquet')

    if not arquivos:
        print(f"\n  {AMARELO}Nenhum arquivo Parquet encontrado em s3/processed/clientes/{RESET}")
        print(f"  Execute primeiro: python3 glue/jobs/clientes_raw_to_processed.py")
        return

    secao("Arquivos em s3/processed/clientes/  (particionado por UF)")
    dfs = []
    for arq in arquivos:
        rel = Path(arq).relative_to(projeto_root)
        tam = tamanho_fmt(os.path.getsize(arq))
        df_part = pd.read_parquet(arq)
        print(f"  {VERDE}✓{RESET}  {rel}  {CINZA}({tam} | {len(df_part)} linhas){RESET}")
        dfs.append(df_part)

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if df.empty:
        print(f"  {AMARELO}(arquivos vazios){RESET}")
        return

    secao(f"Amostra após ETL — {len(df)} clientes processados")
    colunas_show = ['cpf', 'nome', 'email', 'telefone', 'cidade', 'uf',
                    'segmento_cliente', 'limite_credito']
    colunas_show = [c for c in colunas_show if c in df.columns]
    mostrar_tabela(df[colunas_show])

    secao("Schema do arquivo Parquet (tipos de dados)")
    schema_info = []
    for col in df.columns:
        if not col.startswith('_'):
            schema_info.append({
                'Coluna': col,
                'Tipo': str(df[col].dtype),
                'Nulos': df[col].isna().sum(),
                '% Nulos': f"{df[col].isna().mean()*100:.1f}%",
                'Exemplo': str(df[col].dropna().iloc[0]) if not df[col].dropna().empty else 'n/d'
            })
    mostrar_tabela(pd.DataFrame(schema_info))

    secao("Distribuição de limite de crédito por segmento")
    if 'limite_credito' in df.columns and 'segmento_cliente' in df.columns:
        credito = df.groupby('segmento_cliente')['limite_credito'].agg(
            ['count', 'mean', 'min', 'max']
        ).round(2).reset_index()
        credito.columns = ['Segmento', 'Qtd', 'Média (R$)', 'Mínimo (R$)', 'Máximo (R$)']
        mostrar_tabela(credito)

    # Colunas de auditoria
    secao("Colunas de auditoria (metadados do ETL)")
    colunas_meta = [c for c in df.columns if c.startswith('_')]
    if colunas_meta:
        for col in colunas_meta:
            vals = df[col].unique()
            print(f"  {CINZA}{col}: {vals[:2]}{RESET}")


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 4 — S3 processed/vendas (Parquet enriquecido)
# ══════════════════════════════════════════════════════════════════════════════

def etapa4_processed_vendas():
    cabecalho(
        "ETAPA 4 — S3 processed/vendas (Parquet enriquecido)",
        "Após ETL: valores limpos, margem calculada, join com clientes aplicado"
    )

    if not PARQUET_OK:
        print(f"  {VERMELHO}pyarrow não instalado. Execute: pip3 install pyarrow{RESET}")
        return

    arquivos = encontrar_arquivos('s3/processed/vendas/**/*.parquet')

    if not arquivos:
        print(f"\n  {AMARELO}Nenhum Parquet encontrado em s3/processed/vendas/{RESET}")
        print(f"  Execute: python3 glue/jobs/vendas_raw_to_processed.py")
        return

    secao("Arquivos em s3/processed/vendas/  (particionado por ano/mês)")
    dfs = []
    for arq in arquivos:
        rel = Path(arq).relative_to(projeto_root)
        tam = tamanho_fmt(os.path.getsize(arq))
        df_part = pd.read_parquet(arq)
        print(f"  {VERDE}✓{RESET}  {rel}  {CINZA}({tam} | {len(df_part)} linhas){RESET}")
        dfs.append(df_part)

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if df.empty:
        return

    secao(f"Amostra após ETL — {len(df)} vendas processadas")
    colunas_show = ['id_pedido', 'produto_nome', 'valor_total', 'desconto',
                    'categoria_valor', 'margem_pct', 'canal_venda',
                    'forma_pagamento', 'status_venda', 'nome', 'uf']
    colunas_show = [c for c in colunas_show if c in df.columns]
    mostrar_tabela(df[colunas_show])

    secao("Campos calculados pelo ETL (não existiam no raw)")
    campos_novos = ['categoria_valor', 'margem_pct', 'ano_venda', 'mes_venda', 'dia_semana']
    campos_novos = [c for c in campos_novos if c in df.columns]
    if campos_novos:
        mostrar_tabela(df[campos_novos].head(8))

    secao("Receita por categoria de valor")
    if 'categoria_valor' in df.columns:
        cat = df.groupby('categoria_valor').agg(
            Qtd=('id_pedido', 'count'),
            Receita=('valor_total', 'sum'),
            Margem_Media=('margem_pct', 'mean') if 'margem_pct' in df.columns else ('valor_total', 'count')
        ).round(2).reset_index()
        mostrar_tabela(cat)

    secao("Vendas por mês (série temporal)")
    if 'ano_venda' in df.columns and 'mes_venda' in df.columns:
        serie = df.groupby(['ano_venda', 'mes_venda']).agg(
            Qtd_Vendas=('id_pedido', 'count'),
            Receita=('valor_total', 'sum')
        ).round(2).reset_index().sort_values(['ano_venda', 'mes_venda'])
        serie.columns = ['Ano', 'Mês', 'Qtd Vendas', 'Receita (R$)']
        mostrar_tabela(serie, max_rows=15)

    secao("Join com clientes — verificação")
    if 'nome' in df.columns:
        com_nome = df['nome'].notna().sum()
        sem_nome = df['nome'].isna().sum()
        pct = com_nome / len(df) * 100
        print(f"  Vendas com nome do cliente : {VERDE}{com_nome} ({pct:.1f}%){RESET}")
        print(f"  Vendas sem nome (sem match): {AMARELO}{sem_nome}{RESET}")


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 5 — Banco staging
# ══════════════════════════════════════════════════════════════════════════════

def etapa5_staging():
    cabecalho(
        "ETAPA 5 — Banco de dados: Staging",
        "Dados carregados do S3 processed/ para o PostgreSQL (simula Redshift)"
    )

    conn = conectar_banco()
    if not conn:
        return

    try:
        # Verifica tabelas existentes
        secao("Tabelas de staging existentes")
        rodar_query(conn, """
            SELECT table_name,
                   pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS tamanho
            FROM information_schema.tables
            WHERE table_schema = 'staging'
            ORDER BY table_name
        """)

        # Clientes staging
        secao("staging.stg_clientes — amostra")
        rodar_query(conn, """
            SELECT cpf, nome, email, cidade, uf, segmento_cliente, limite_credito
            FROM staging.stg_clientes
            LIMIT 8
        """)

        rodar_query(conn, """
            SELECT COUNT(*) AS total_clientes,
                   COUNT(DISTINCT uf) AS estados_distintos,
                   SUM(CASE WHEN segmento_cliente = 'B2B' THEN 1 ELSE 0 END) AS b2b,
                   SUM(CASE WHEN segmento_cliente = 'B2C' THEN 1 ELSE 0 END) AS b2c,
                   ROUND(AVG(limite_credito), 2) AS limite_medio
            FROM staging.stg_clientes
        """, titulo="staging.stg_clientes — totais")

        # Vendas staging
        secao("staging.stg_vendas — amostra")
        rodar_query(conn, """
            SELECT id_pedido, produto_nome, valor_total, canal_venda,
                   forma_pagamento, status_venda, data_venda::DATE
            FROM staging.stg_vendas
            LIMIT 8
        """)

        rodar_query(conn, """
            SELECT COUNT(*) AS total_vendas,
                   ROUND(SUM(valor_total), 2) AS receita_total,
                   ROUND(AVG(valor_total), 2) AS ticket_medio,
                   COUNT(DISTINCT cpf_cliente) AS clientes_compradores
            FROM staging.stg_vendas
        """, titulo="staging.stg_vendas — totais")

    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 6 — Banco dw (Star Schema)
# ══════════════════════════════════════════════════════════════════════════════

def etapa6_dw():
    cabecalho(
        "ETAPA 6 — Banco de dados: Data Warehouse (Star Schema)",
        "Modelo dimensional com dimensões e tabela fato"
    )

    conn = conectar_banco()
    if not conn:
        return

    try:
        secao("Tabelas do DW e quantidade de registros")
        rodar_query(conn, """
            SELECT t.table_name AS tabela,
                   (SELECT COUNT(*) FROM information_schema.columns c
                    WHERE c.table_schema = 'dw' AND c.table_name = t.table_name) AS colunas,
                   pg_size_pretty(pg_total_relation_size('dw.' || t.table_name)) AS tamanho
            FROM information_schema.tables t
            WHERE t.table_schema = 'dw'
            ORDER BY t.table_name
        """)

        secao("dw.dim_clientes — amostra")
        rodar_query(conn, """
            SELECT sk_cliente, cpf, nome, cidade, uf, regiao,
                   segmento_cliente, faixa_etaria, registro_atual
            FROM dw.dim_clientes
            WHERE registro_atual = TRUE
            LIMIT 8
        """)

        secao("dw.dim_clientes — distribuição por região")
        rodar_query(conn, """
            SELECT regiao,
                   COUNT(*) AS qtd_clientes,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
            FROM dw.dim_clientes
            WHERE registro_atual = TRUE
            GROUP BY regiao
            ORDER BY qtd_clientes DESC
        """)

        secao("dw.dim_tempo — amostra (calendário analítico)")
        rodar_query(conn, """
            SELECT sk_tempo, data_completa, ano, mes, nome_mes,
                   trimestre, dia_semana, nome_dia, flag_fim_semana
            FROM dw.dim_tempo
            ORDER BY data_completa
            LIMIT 8
        """)

        secao("dw.fato_vendas — amostra com joins")
        rodar_query(conn, """
            SELECT f.sk_venda,
                   t.data_completa AS data,
                   c.nome AS cliente,
                   c.uf,
                   f.valor_total,
                   f.desconto,
                   f.margem_pct,
                   f.canal_venda,
                   f.status_venda
            FROM dw.fato_vendas f
            JOIN dw.dim_clientes c ON f.sk_cliente = c.sk_cliente
            JOIN dw.dim_tempo t    ON f.sk_tempo = t.sk_tempo
            WHERE c.registro_atual = TRUE
            ORDER BY f.valor_total DESC
            LIMIT 8
        """)

        secao("dw.fato_vendas — receita por trimestre")
        rodar_query(conn, """
            SELECT t.ano,
                   t.trimestre,
                   COUNT(*) AS qtd_vendas,
                   ROUND(SUM(f.valor_total), 2) AS receita,
                   ROUND(AVG(f.valor_total), 2) AS ticket_medio,
                   ROUND(AVG(f.margem_pct), 2) AS margem_media_pct
            FROM dw.fato_vendas f
            JOIN dw.dim_tempo t ON f.sk_tempo = t.sk_tempo
            GROUP BY t.ano, t.trimestre
            ORDER BY t.ano, t.trimestre
        """)

    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 7 — Banco dm (Data Marts — views para BI)
# ══════════════════════════════════════════════════════════════════════════════

def etapa7_dm():
    cabecalho(
        "ETAPA 7 — Banco de dados: Data Marts (views para BI)",
        "Views prontas para o time de BI e relatórios"
    )

    conn = conectar_banco()
    if not conn:
        return

    try:
        secao("Views disponíveis no schema dm")
        rodar_query(conn, """
            SELECT table_name AS view_name
            FROM information_schema.views
            WHERE table_schema = 'dm'
            ORDER BY table_name
        """)

        secao("dm.vm_vendas_diarias — últimos 10 dias")
        rodar_query(conn, """
            SELECT * FROM dm.vm_vendas_diarias
            ORDER BY data DESC
            LIMIT 10
        """)

        secao("dm.vm_top_produtos — top 10 por receita")
        rodar_query(conn, """
            SELECT * FROM dm.vm_top_produtos
            LIMIT 10
        """)

        secao("dm.vm_vendas_por_uf — mapa de calor por estado")
        rodar_query(conn, """
            SELECT * FROM dm.vm_vendas_por_uf
            ORDER BY receita_total DESC
            LIMIT 10
        """)

        secao("dm.vm_desempenho_canais — comparativo de canais")
        rodar_query(conn, """
            SELECT * FROM dm.vm_desempenho_canais
            ORDER BY receita_total DESC
        """)

    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# SQL LIVRE
# ══════════════════════════════════════════════════════════════════════════════

def modo_sql():
    cabecalho(
        "SQL Interativo",
        "Digite qualquer SQL para rodar no banco. 'sair' para fechar."
    )

    conn = conectar_banco()
    if not conn:
        return

    print(f"""
  {CINZA}Schemas disponíveis:
    staging   →  stg_clientes, stg_vendas
    dw        →  dim_clientes, dim_produto, dim_tempo, fato_vendas
    dm        →  vm_vendas_diarias, vm_top_produtos, vm_vendas_por_uf

  Exemplos:
    SELECT * FROM dw.dim_clientes LIMIT 5;
    SELECT canal_venda, SUM(valor_total) FROM dw.fato_vendas GROUP BY 1;
    \\dt staging.*      (listar tabelas)
  {RESET}""")

    historico = []
    while True:
        try:
            print(f"\n{AZUL}sql>{RESET} ", end='', flush=True)
            linhas = []
            while True:
                linha = input()
                if linha.strip().lower() in ('sair', 'exit', 'quit', '\\q'):
                    print(f"  {CINZA}Saindo do modo SQL.{RESET}")
                    conn.close()
                    return
                linhas.append(linha)
                if linha.strip().endswith(';') or linha.strip() == '':
                    break

            sql = ' '.join(linhas).strip().rstrip(';')
            if not sql:
                continue

            historico.append(sql)
            inicio = datetime.now()

            try:
                df = pd.read_sql(sql, conn)
                duracao = (datetime.now() - inicio).total_seconds()
                print()
                mostrar_tabela(df, max_rows=50)
                print(f"  {CINZA}({len(df)} linha(s) em {duracao:.3f}s){RESET}")
            except Exception as e:
                print(f"  {VERMELHO}Erro: {e}{RESET}")
                conn.rollback()

        except (KeyboardInterrupt, EOFError):
            print(f"\n  {CINZA}Saindo.{RESET}")
            conn.close()
            return


# ══════════════════════════════════════════════════════════════════════════════
# MENU INTERATIVO
# ══════════════════════════════════════════════════════════════════════════════

ETAPAS = {
    '1':   ('S3 raw/clientes (JSON bruto)',          etapa1_raw_clientes),
    '2':   ('S3 raw/vendas (CSV bruto)',              etapa2_raw_vendas),
    '3':   ('S3 processed/clientes (Parquet limpo)', etapa3_processed_clientes),
    '4':   ('S3 processed/vendas (Parquet enriq.)',  etapa4_processed_vendas),
    '5':   ('Banco: staging (área de chegada)',       etapa5_staging),
    '6':   ('Banco: dw — Star Schema',               etapa6_dw),
    '7':   ('Banco: dm — Data Marts (views BI)',     etapa7_dm),
    'sql': ('SQL interativo livre',                  modo_sql),
    'all': ('Todas as etapas S3 (sem banco)',         None),
}


def menu():
    while True:
        print(f"""
{AZUL}{NEGRITO}╔══════════════════════════════════════════════════════════╗
║       VISUALIZADOR DA PLATAFORMA DE DADOS              ║
╚══════════════════════════════════════════════════════════╝{RESET}

  {NEGRITO}Etapas S3 (arquivo — funciona sem Docker):{RESET}
    {VERDE}1{RESET}  — S3 raw/clientes       (JSON bruto, antes do ETL)
    {VERDE}2{RESET}  — S3 raw/vendas         (CSV bruto, antes do ETL)
    {VERDE}3{RESET}  — S3 processed/clientes (Parquet, após ETL)
    {VERDE}4{RESET}  — S3 processed/vendas   (Parquet, após ETL + join)

  {NEGRITO}Banco de dados (requer Docker + PostgreSQL):{RESET}
    {AMARELO}5{RESET}  — Staging    (dados como chegam no banco)
    {AMARELO}6{RESET}  — DW         (Star Schema — dimensões + fato)
    {AMARELO}7{RESET}  — Data Marts (views prontas para BI)
    {AMARELO}sql{RESET} — SQL livre  (escreva qualquer query)

  {NEGRITO}Outros:{RESET}
    {CIANO}all{RESET} — Todas as etapas S3 de uma vez
    {VERMELHO}sair{RESET} — Encerrar

""")
        escolha = input(f"  {NEGRITO}Escolha:{RESET} ").strip().lower()

        if escolha in ('sair', 'exit', 'q'):
            print(f"\n  {CINZA}Até logo!{RESET}\n")
            break
        elif escolha == 'all':
            etapa1_raw_clientes()
            etapa2_raw_vendas()
            etapa3_processed_clientes()
            etapa4_processed_vendas()
        elif escolha in ETAPAS:
            _, fn = ETAPAS[escolha]
            if fn:
                fn()
        else:
            print(f"  {VERMELHO}Opção inválida. Tente: 1, 2, 3, 4, 5, 6, 7, sql, all ou sair{RESET}")

        input(f"\n  {CINZA}[ Enter para voltar ao menu ]  {RESET}")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Visualiza dados de cada etapa do pipeline'
    )
    parser.add_argument(
        '--etapa',
        choices=['1', '2', '3', '4', '5', '6', '7', 'sql', 'all'],
        help='Etapa para visualizar direto (sem menu)'
    )
    args = parser.parse_args()

    if args.etapa:
        if args.etapa == 'all':
            etapa1_raw_clientes()
            etapa2_raw_vendas()
            etapa3_processed_clientes()
            etapa4_processed_vendas()
        elif args.etapa == 'sql':
            modo_sql()
        else:
            _, fn = ETAPAS[args.etapa]
            fn()
    else:
        menu()
