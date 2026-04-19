"""
crawler_simulator.py - Simula o AWS Glue Crawler

CONCEITO AWS GLUE CRAWLER:
    O Glue Crawler é um programa automático que:
    1. Varre o S3 em busca de arquivos (JSON, CSV, Parquet, etc.)
    2. Infere automaticamente o schema (colunas e tipos)
    3. Cria/atualiza tabelas no Data Catalog
    4. Detecta novas partições

    É como um "detetive de dados" que descobre o que está no seu S3.

    No AWS real:
        - Configurado via console: Glue → Crawlers → Create Crawler
        - Agendado para rodar periodicamente (ex: às 02:00 todo dia)
        - Custo: $0.44/DPU-hora (mínimo 10 minutos = $0.073)

    Aqui simulamos esse comportamento localmente.

USO:
    python crawler_simulator.py                    # Varre toda a pasta s3/raw/
    python crawler_simulator.py --prefix raw/clientes/   # Varre pasta específica
    python crawler_simulator.py --output catalog/        # Salva schema em pasta
"""

import os
import sys
import json
import csv
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
import io

import pandas as pd
import pyarrow.parquet as pq

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

from s3.scripts.s3_utils import S3Local

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [CRAWLER] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('GlueCrawler')


# Mapeamento de tipos Python/Pandas para tipos do Glue/Hive
TIPO_MAPEAMENTO = {
    'int64': 'bigint',
    'int32': 'int',
    'float64': 'double',
    'float32': 'float',
    'bool': 'boolean',
    'object': 'string',
    'datetime64[ns]': 'timestamp',
    'datetime64[ns, UTC]': 'timestamp',
}


def inferir_tipo_glue(pandas_tipo: str, valores_amostra: list = None) -> str:
    """
    Converte tipo Pandas para tipo Glue/Hive.

    No Glue real, o Spark faz essa inferência automaticamente via:
        spark.read.option("inferSchema", "true").json(path)

    Aqui fazemos manualmente.
    """
    tipo_str = str(pandas_tipo).lower()

    # Mapeia diretamente
    if tipo_str in TIPO_MAPEAMENTO:
        return TIPO_MAPEAMENTO[tipo_str]

    # Para strings, tenta inferir tipo mais específico
    if tipo_str == 'object' and valores_amostra:
        amostra_limpa = [v for v in valores_amostra if v and str(v) != 'nan']
        if not amostra_limpa:
            return 'string'

        # Tenta int
        try:
            [int(v) for v in amostra_limpa[:10]]
            return 'bigint'
        except (ValueError, TypeError):
            pass

        # Tenta float
        try:
            [float(str(v).replace(',', '.').replace('R$', '').strip())
             for v in amostra_limpa[:10]]
            return 'double'
        except (ValueError, TypeError):
            pass

        # Tenta data
        try:
            pd.to_datetime(amostra_limpa[:5])
            return 'timestamp'
        except (ValueError, TypeError):
            pass

    return TIPO_MAPEAMENTO.get(tipo_str, 'string')


def analisar_arquivo_json(conteudo_bytes: bytes,
                          key: str) -> Optional[Dict]:
    """
    Analisa um arquivo JSON e infere seu schema.

    O Glue Crawler faz isso com Apache Spark:
        spark.read.json(path).schema
    """
    try:
        conteudo = conteudo_bytes.decode('utf-8')
        dados = json.loads(conteudo)

        # Normaliza para lista de registros
        if isinstance(dados, list):
            registros = dados
        elif isinstance(dados, dict):
            # Procura por uma lista dentro do dict
            for valor in dados.values():
                if isinstance(valor, list) and valor:
                    registros = valor
                    break
            else:
                registros = [dados]
        else:
            return None

        if not registros:
            return None

        # Usa Pandas para inferir schema
        df = pd.DataFrame(registros)

        colunas = []
        for col in df.columns:
            if col.startswith('_'):  # Pula metadados internos
                continue
            amostra = df[col].dropna().tolist()[:20]
            tipo = inferir_tipo_glue(str(df[col].dtype), amostra)
            colunas.append({
                'Name': col,
                'Type': tipo,
                'Comment': f'Campo inferido do arquivo JSON'
            })

        return {
            'formato': 'JSON',
            'total_registros': len(registros),
            'colunas': colunas,
            'arquivo': key
        }

    except Exception as e:
        logger.warning(f"  Erro ao analisar JSON {key}: {e}")
        return None


def analisar_arquivo_csv(conteudo_bytes: bytes, key: str) -> Optional[Dict]:
    """
    Analisa um arquivo CSV e infere seu schema.
    """
    try:
        conteudo = conteudo_bytes.decode('utf-8')
        df = pd.read_csv(io.StringIO(conteudo), nrows=1000)  # Lê só 1000 linhas para amostra

        colunas = []
        for col in df.columns:
            amostra = df[col].dropna().tolist()[:20]
            tipo = inferir_tipo_glue(str(df[col].dtype), amostra)
            colunas.append({
                'Name': col.strip(),
                'Type': tipo,
                'Comment': f'Campo inferido do arquivo CSV'
            })

        return {
            'formato': 'CSV',
            'total_registros': len(df),
            'total_colunas': len(df.columns),
            'colunas': colunas,
            'arquivo': key
        }

    except Exception as e:
        logger.warning(f"  Erro ao analisar CSV {key}: {e}")
        return None


def analisar_arquivo_parquet(conteudo_bytes: bytes, key: str) -> Optional[Dict]:
    """
    Analisa um arquivo Parquet. Parquet JÁ TEM o schema embutido!

    Uma das grandes vantagens do Parquet: o Crawler não precisa inferir,
    ele simplesmente lê o schema do metadata do arquivo.
    """
    try:
        buffer = io.BytesIO(conteudo_bytes)
        schema_parquet = pq.read_schema(buffer)

        colunas = []
        for field in schema_parquet:
            tipo_parquet = str(field.type)
            # Mapeamento simplificado Parquet → Glue
            if 'int64' in tipo_parquet or 'int32' in tipo_parquet:
                tipo_glue = 'bigint'
            elif 'float' in tipo_parquet or 'double' in tipo_parquet:
                tipo_glue = 'double'
            elif 'bool' in tipo_parquet:
                tipo_glue = 'boolean'
            elif 'timestamp' in tipo_parquet:
                tipo_glue = 'timestamp'
            else:
                tipo_glue = 'string'

            colunas.append({
                'Name': field.name,
                'Type': tipo_glue,
                'Comment': f'Campo do Parquet (tipo original: {tipo_parquet})'
            })

        buffer.seek(0)
        parquet_file = pq.ParquetFile(buffer)
        total_rows = parquet_file.metadata.num_rows

        return {
            'formato': 'Parquet',
            'total_registros': total_rows,
            'colunas': colunas,
            'arquivo': key
        }

    except Exception as e:
        logger.warning(f"  Erro ao analisar Parquet {key}: {e}")
        return None


def detectar_particoes(objetos: List[Dict]) -> List[str]:
    """
    Detecta o padrão de particionamento dos arquivos.

    No Glue, o Crawler detecta partições Hive automaticamente:
        raw/clientes/ano=2024/mes=01/dia=15/arquivo.json
        → Partições: ano, mes, dia

    Retorna lista de campos de partição detectados.
    """
    particoes_encontradas = set()

    for obj in objetos:
        key = obj['Key']
        partes = key.split('/')

        for parte in partes:
            # Padrão Hive: campo=valor
            if '=' in parte:
                campo = parte.split('=')[0]
                particoes_encontradas.add(campo)

    return sorted(list(particoes_encontradas))


class GlueCrawlerSimulado:
    """
    Simula o comportamento do AWS Glue Crawler.

    No AWS:
        - Configurado com IAM role que permite acesso ao S3
        - Agendado via cron ou trigger
        - Resultado: tabelas no Glue Data Catalog
        - Pode ser consultado via Athena ou Redshift Spectrum

    Local:
        - Varre a pasta s3/
        - Gera schemas JSON na pasta glue/catalog/
    """

    def __init__(self, base_path: str = None):
        self.s3 = S3Local(base_path)
        self.catalog = {}  # Armazena schemas descobertos
        self.estatisticas = {
            'arquivos_varridos': 0,
            'tabelas_criadas': 0,
            'tabelas_atualizadas': 0,
            'erros': 0,
            'inicio': datetime.now().isoformat()
        }

    def run(self, prefix: str = '') -> Dict:
        """
        Executa o crawler: varre o S3 e infere schemas.

        Returns:
            Catálogo com schemas de todas as tabelas encontradas
        """
        logger.info("=" * 60)
        logger.info("INICIANDO GLUE CRAWLER SIMULADO")
        logger.info(f"Varrendo prefixo: '{prefix or '(tudo)'}' ")
        logger.info("=" * 60)

        # Lista todos os arquivos
        objetos = self.s3.list_objects(prefix=prefix)
        logger.info(f"Total de objetos encontrados: {len(objetos)}")

        # Agrupa por "tabela" (baseado no prefixo)
        tabelas_detectadas = self._detectar_tabelas(objetos)
        logger.info(f"Tabelas detectadas: {len(tabelas_detectadas)}")

        # Analisa cada tabela
        for nome_tabela, arquivos_tabela in tabelas_detectadas.items():
            logger.info(f"\nAnalisando tabela: {nome_tabela}")
            schema = self._analisar_tabela(nome_tabela, arquivos_tabela)

            if schema:
                self.catalog[nome_tabela] = schema
                self.estatisticas['tabelas_criadas'] += 1

        # Salva catálogo
        self._salvar_catalog()

        logger.info("\n" + "=" * 60)
        logger.info("CRAWLER CONCLUÍDO")
        logger.info(f"  Arquivos varridos  : {self.estatisticas['arquivos_varridos']}")
        logger.info(f"  Tabelas criadas    : {self.estatisticas['tabelas_criadas']}")
        logger.info(f"  Erros              : {self.estatisticas['erros']}")
        logger.info("=" * 60)

        return self.catalog

    def _detectar_tabelas(self, objetos: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Agrupa arquivos por "tabela" baseado no prefixo do caminho.

        Exemplo:
            raw/clientes/2024/01/arquivo1.json  → tabela: raw_clientes
            raw/clientes/2024/02/arquivo2.json  → tabela: raw_clientes
            raw/vendas/2024/01/arquivo1.csv     → tabela: raw_vendas
        """
        tabelas = {}

        for obj in objetos:
            key = obj['Key']
            partes = key.split('/')

            # Remove partes de partição (campo=valor) e o arquivo em si
            partes_tabela = []
            for parte in partes[:-1]:  # Exclui o arquivo
                if '=' not in parte:   # Exclui partições (ano=2024, mes=01)
                    partes_tabela.append(parte)

            if not partes_tabela:
                continue

            # Nome da tabela = prefixos sem partição, unidos por underscore
            nome_tabela = '_'.join(partes_tabela)
            nome_tabela = nome_tabela.replace('-', '_')

            if nome_tabela not in tabelas:
                tabelas[nome_tabela] = []
            tabelas[nome_tabela].append(obj)

        return tabelas

    def _analisar_tabela(self, nome_tabela: str,
                          arquivos: List[Dict]) -> Optional[Dict]:
        """
        Analisa os arquivos de uma tabela e monta o schema.
        Usa amostra de arquivos para inferência (como o Glue real faz).
        """
        if not arquivos:
            return None

        # Usa até 5 arquivos como amostra (Glue usa mais, mas para simulação está ok)
        amostra = arquivos[:5]
        schemas_amostra = []

        for obj in amostra:
            key = obj['Key']
            self.estatisticas['arquivos_varridos'] += 1

            logger.info(f"  Analisando: {key}")

            response = self.s3.get_object(key)
            if not response:
                continue

            conteudo_bytes = response['Body']

            # Detecta formato pelo sufixo
            if key.endswith('.json'):
                schema = analisar_arquivo_json(conteudo_bytes, key)
            elif key.endswith('.csv'):
                schema = analisar_arquivo_csv(conteudo_bytes, key)
            elif key.endswith('.parquet'):
                schema = analisar_arquivo_parquet(conteudo_bytes, key)
            else:
                logger.info(f"  Formato não suportado: {key}")
                continue

            if schema:
                schemas_amostra.append(schema)

        if not schemas_amostra:
            return None

        # Mescla schemas (union de colunas)
        colunas_mergeadas = self._merge_schemas(schemas_amostra)

        # Detecta partições
        particoes = detectar_particoes(arquivos)

        # Detecta localização (prefixo sem partições)
        localizacao = self._detectar_localizacao(arquivos[0]['Key'])

        schema_final = {
            'nome_tabela': nome_tabela,
            'database': 'local_catalog',
            'localizacao': localizacao,
            'localizacao_local': f"./{localizacao}",
            'formato': schemas_amostra[0]['formato'],
            'colunas': colunas_mergeadas,
            'particoes': [{'Name': p, 'Type': 'string'} for p in particoes],
            'total_arquivos': len(arquivos),
            'amostra_usada': len(schemas_amostra),
            'ultima_atualizacao': datetime.now().isoformat(),
            '_crawler': 'GlueCrawlerSimulado v1.0'
        }

        return schema_final

    def _merge_schemas(self, schemas: List[Dict]) -> List[Dict]:
        """
        Mescla schemas de múltiplos arquivos em um único schema.
        Resolve conflitos de tipo (escolhe o mais abrangente).
        """
        colunas_merged = {}

        for schema in schemas:
            for col in schema.get('colunas', []):
                nome = col['Name']
                if nome not in colunas_merged:
                    colunas_merged[nome] = col.copy()
                else:
                    # Resolução de conflito: preferir string (mais genérico)
                    tipo_existente = colunas_merged[nome]['Type']
                    tipo_novo = col['Type']

                    if tipo_existente != tipo_novo:
                        # string é o tipo mais genérico
                        if 'string' in (tipo_existente, tipo_novo):
                            colunas_merged[nome]['Type'] = 'string'
                        elif 'double' in (tipo_existente, tipo_novo):
                            colunas_merged[nome]['Type'] = 'double'

        return list(colunas_merged.values())

    def _detectar_localizacao(self, key_exemplo: str) -> str:
        """Extrai a localização base (sem partições e sem arquivo)."""
        partes = key_exemplo.split('/')
        partes_base = []
        for parte in partes[:-1]:
            if '=' in parte:
                break
            partes_base.append(parte)
        return '/'.join(partes_base) + '/'

    def _salvar_catalog(self):
        """Salva o catálogo descoberto como JSON."""
        catalog_path = projeto_root / 'glue' / 'catalog' / 'discovered'
        catalog_path.mkdir(parents=True, exist_ok=True)

        for nome_tabela, schema in self.catalog.items():
            arquivo = catalog_path / f"{nome_tabela}.json"
            with open(arquivo, 'w', encoding='utf-8') as f:
                json.dump(schema, f, ensure_ascii=False, indent=2)
            logger.info(f"  Schema salvo: {arquivo}")

        # Salva resumo do catálogo
        resumo = {
            'timestamp': datetime.now().isoformat(),
            'total_tabelas': len(self.catalog),
            'tabelas': list(self.catalog.keys()),
            'estatisticas': self.estatisticas
        }
        with open(catalog_path / '_catalog_summary.json', 'w') as f:
            json.dump(resumo, f, ensure_ascii=False, indent=2)


# =============================================================================
# PONTO DE ENTRADA
# =============================================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Glue Crawler Simulado - descobre schemas automaticamente'
    )
    parser.add_argument(
        '--prefix',
        default='',
        help='Prefixo S3 para varrer (ex: raw/clientes/). Padrão: tudo'
    )
    args = parser.parse_args()

    crawler = GlueCrawlerSimulado()
    catalogo = crawler.run(prefix=args.prefix)

    print(f"\nTabelas descobertas: {len(catalogo)}")
    for nome, schema in catalogo.items():
        print(f"  - {nome}: {len(schema['colunas'])} colunas, "
              f"{schema['total_arquivos']} arquivos")
