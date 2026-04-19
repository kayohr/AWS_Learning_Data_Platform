"""
validate_schema.py - Lambda: Valida schema de arquivos recém-chegados no S3

CONCEITO AWS:
    Esta função seria implantada como AWS Lambda:
        - Runtime: Python 3.11
        - Memory: 256MB (suficiente para arquivos pequenos)
        - Timeout: 60 segundos
        - Trigger: S3 Event Notification (s3:ObjectCreated:*)

    O Lambda valida se o arquivo tem as colunas obrigatórias.
    Se inválido, move para s3/raw/quarentena/ e envia alerta.

FLUXO:
    S3 raw/ → NOVO ARQUIVO → Lambda validate_schema → VÁLIDO? →
        SIM: Segue o pipeline normal
        NÃO: Move para s3/raw/quarentena/ + SNS notification

HANDLER:
    Todo Lambda tem um "handler" que é o ponto de entrada.
    No AWS: você configura lambda_handler como o handler da função.
"""

import os
import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

import pandas as pd

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

from s3.scripts.s3_utils import S3Local

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [LAMBDA:validate_schema] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('Lambda-ValidateSchema')


# =============================================================================
# SCHEMAS OBRIGATÓRIOS POR FONTE
# No AWS: você poderia armazenar isso no Glue Data Catalog ou Parameter Store
# =============================================================================
SCHEMAS_OBRIGATORIOS = {
    'clientes': {
        'colunas_obrigatorias': ['cpf', 'nome'],
        'colunas_esperadas': ['cpf', 'nome', 'email', 'telefone',
                               'cidade', 'uf', 'data_cadastro'],
        'regras': {
            'cpf': {
                'tipo': 'string',
                'tamanho_min': 11,
                'tamanho_max': 14,
                'obrigatorio': True
            },
            'nome': {
                'tipo': 'string',
                'tamanho_min': 3,
                'obrigatorio': True
            }
        }
    },
    'vendas': {
        'colunas_obrigatorias': ['id_venda', 'cpf_cliente', 'valor_total', 'data_venda'],
        'colunas_esperadas': ['id_venda', 'cpf_cliente', 'valor_total', 'data_venda',
                               'produto_nome', 'quantidade', 'canal_venda'],
        'regras': {
            'valor_total': {
                'tipo': 'numeric',
                'valor_min': 0.01,
                'obrigatorio': True
            }
        }
    },
    'eventos': {
        'colunas_obrigatorias': ['evento_id', 'tipo_evento', 'timestamp'],
        'colunas_esperadas': ['evento_id', 'tipo_evento', 'timestamp',
                               'user_id', 'sessao_id'],
        'regras': {}
    }
}


class ResultadoValidacao:
    """
    Resultado de uma validação de schema.
    Similar a um "validation report" do Great Expectations.
    """

    def __init__(self, key: str):
        self.key = key
        self.valido = True
        self.erros = []
        self.avisos = []
        self.metricas = {
            'total_registros': 0,
            'colunas_encontradas': [],
            'colunas_faltando': [],
            'colunas_extras': [],
            'registros_invalidos': 0
        }
        self.timestamp = datetime.now().isoformat()

    def adicionar_erro(self, mensagem: str):
        """Adiciona erro crítico (invalida o arquivo)."""
        self.erros.append(mensagem)
        self.valido = False

    def adicionar_aviso(self, mensagem: str):
        """Adiciona aviso (não invalida, mas registra problema)."""
        self.avisos.append(mensagem)

    def to_dict(self) -> Dict:
        return {
            'key': self.key,
            'valido': self.valido,
            'erros': self.erros,
            'avisos': self.avisos,
            'metricas': self.metricas,
            'timestamp': self.timestamp
        }


def detectar_fonte(key: str) -> Optional[str]:
    """
    Detecta qual fonte de dados é baseado no path do S3.

    Ex: 'raw/clientes/2024/01/arquivo.json' → 'clientes'
        'raw/vendas/2024/01/arquivo.csv'    → 'vendas'
        'raw/eventos/2024/01/arquivo.json'  → 'eventos'
    """
    partes = key.lower().split('/')
    for fonte in SCHEMAS_OBRIGATORIOS.keys():
        if fonte in partes:
            return fonte
    return None


def ler_arquivo_como_df(s3: S3Local, key: str) -> Optional[pd.DataFrame]:
    """
    Lê um arquivo do S3 como DataFrame.
    Suporta JSON e CSV.
    """
    import io

    response = s3.get_object(key)
    if not response:
        return None

    conteudo = response['Body'].decode('utf-8')

    try:
        if key.endswith('.json'):
            dados = json.loads(conteudo)
            if isinstance(dados, list):
                return pd.DataFrame(dados)
            elif isinstance(dados, dict):
                for chave, val in dados.items():
                    if isinstance(val, list):
                        return pd.DataFrame(val)
                return pd.DataFrame([dados])
        elif key.endswith('.csv'):
            return pd.read_csv(io.StringIO(conteudo), nrows=100)
        else:
            return None
    except Exception as e:
        logger.error(f"Erro ao ler arquivo {key}: {e}")
        return None


def validar_colunas(df: pd.DataFrame, fonte: str,
                    resultado: ResultadoValidacao):
    """
    Valida se o DataFrame tem as colunas obrigatórias.
    """
    schema = SCHEMAS_OBRIGATORIOS.get(fonte, {})
    colunas_obrigatorias = schema.get('colunas_obrigatorias', [])
    colunas_esperadas = schema.get('colunas_esperadas', [])

    colunas_arquivo = [c.lower() for c in df.columns]
    resultado.metricas['colunas_encontradas'] = list(df.columns)

    # Verifica colunas obrigatórias
    for col in colunas_obrigatorias:
        if col.lower() not in colunas_arquivo:
            resultado.adicionar_erro(
                f"Coluna obrigatória ausente: '{col}'"
            )

    # Verifica colunas esperadas (não obrigatórias, mas esperadas)
    colunas_faltando = [c for c in colunas_esperadas
                        if c.lower() not in colunas_arquivo]
    if colunas_faltando:
        resultado.metricas['colunas_faltando'] = colunas_faltando
        resultado.adicionar_aviso(
            f"Colunas esperadas ausentes: {colunas_faltando}"
        )

    # Detecta colunas extras (não esperadas)
    colunas_extras = [c for c in df.columns
                      if c.lower() not in [e.lower() for e in colunas_esperadas]]
    if colunas_extras:
        resultado.metricas['colunas_extras'] = colunas_extras


def validar_regras(df: pd.DataFrame, fonte: str,
                   resultado: ResultadoValidacao):
    """
    Aplica regras de validação por campo.
    """
    schema = SCHEMAS_OBRIGATORIOS.get(fonte, {})
    regras = schema.get('regras', {})

    for campo, regra in regras.items():
        if campo not in df.columns:
            continue

        col = df[campo]

        # Valida obrigatoriedade (sem nulos)
        if regra.get('obrigatorio') and col.isna().any():
            qtd_nulos = col.isna().sum()
            resultado.adicionar_erro(
                f"Campo obrigatório '{campo}' tem {qtd_nulos} valores nulos"
            )

        # Valida tamanho mínimo
        if regra.get('tamanho_min') and col.dtype == 'object':
            muito_curtos = (col.dropna().str.len() < regra['tamanho_min']).sum()
            if muito_curtos > 0:
                resultado.adicionar_aviso(
                    f"Campo '{campo}': {muito_curtos} valores menores que "
                    f"{regra['tamanho_min']} caracteres"
                )

        # Valida valor mínimo (numérico)
        if regra.get('valor_min') and regra.get('tipo') == 'numeric':
            try:
                valores_numericos = pd.to_numeric(
                    col.astype(str).str.replace(r'[R$\s.,]', '', regex=True),
                    errors='coerce'
                )
                abaixo_minimo = (valores_numericos < regra['valor_min']).sum()
                if abaixo_minimo > 0:
                    resultado.adicionar_erro(
                        f"Campo '{campo}': {abaixo_minimo} valores abaixo do mínimo "
                        f"({regra['valor_min']})"
                    )
            except Exception:
                pass


def mover_para_quarentena(s3: S3Local, key: str,
                          resultado: ResultadoValidacao) -> str:
    """
    Move arquivo inválido para quarentena.

    No AWS: o arquivo seria movido para s3://bucket/quarentena/
    e uma notificação seria enviada via SNS.

    Returns:
        Nova key do arquivo em quarentena
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nome_arquivo = key.split('/')[-1]
    quarentena_key = f"raw/quarentena/{timestamp}_{nome_arquivo}"

    try:
        s3.move_object(key, quarentena_key)
        logger.warning(f"Arquivo movido para quarentena: {quarentena_key}")

        # Salva relatório de quarentena
        relatorio_key = quarentena_key.replace(nome_arquivo, f"_relatorio_{nome_arquivo}.json")
        s3.put_object(relatorio_key, resultado.to_dict())

    except Exception as e:
        logger.error(f"Erro ao mover para quarentena: {e}")

    return quarentena_key


def lambda_handler(event: Dict, context: Any = None) -> Dict:
    """
    Handler principal da função Lambda.

    Este é o PONTO DE ENTRADA do Lambda.
    No AWS, você configura: Handler = validate_schema.lambda_handler

    Args:
        event: Evento S3 com informações do arquivo criado
        context: Contexto Lambda (timeout, request_id, etc.) - None na simulação

    Returns:
        Dict com resultado da validação
        Status 200 = válido
        Status 400 = inválido (arquivo movido para quarentena)
        Status 500 = erro no processamento
    """
    logger.info("Lambda invocado")
    logger.info(f"Evento recebido: {json.dumps(event, indent=2)[:500]}")

    s3 = S3Local()
    resultados = []

    try:
        # Processa cada record do evento S3 (pode vir em batch)
        for record in event.get('Records', []):
            # Extrai informações do arquivo
            key = record['s3']['object']['key']
            file_size = record['s3']['object'].get('size', 0)

            logger.info(f"Validando arquivo: {key} ({file_size} bytes)")

            resultado = ResultadoValidacao(key)

            # Detecta a fonte de dados pelo path
            fonte = detectar_fonte(key)
            if not fonte:
                resultado.adicionar_aviso(
                    f"Fonte desconhecida para o path: {key}. "
                    f"Fontes conhecidas: {list(SCHEMAS_OBRIGATORIOS.keys())}"
                )
                resultados.append(resultado.to_dict())
                continue

            # Lê o arquivo
            df = ler_arquivo_como_df(s3, key)
            if df is None:
                resultado.adicionar_erro(f"Não foi possível ler o arquivo: {key}")
                mover_para_quarentena(s3, key, resultado)
                resultados.append(resultado.to_dict())
                continue

            resultado.metricas['total_registros'] = len(df)
            logger.info(f"  {len(df)} registros lidos, fonte: {fonte}")

            # Verifica se tem dados
            if df.empty:
                resultado.adicionar_erro("Arquivo vazio (zero registros)")
                mover_para_quarentena(s3, key, resultado)
                resultados.append(resultado.to_dict())
                continue

            # Valida colunas
            validar_colunas(df, fonte, resultado)

            # Valida regras de campo
            validar_regras(df, fonte, resultado)

            # Se inválido, move para quarentena
            if not resultado.valido:
                logger.error(f"Arquivo INVÁLIDO: {key}")
                for erro in resultado.erros:
                    logger.error(f"  ERRO: {erro}")
                mover_para_quarentena(s3, key, resultado)
            else:
                logger.info(f"Arquivo VÁLIDO: {key}")
                if resultado.avisos:
                    for aviso in resultado.avisos:
                        logger.warning(f"  AVISO: {aviso}")

            resultados.append(resultado.to_dict())

        # Resposta da função Lambda
        todos_validos = all(r['valido'] for r in resultados)

        return {
            'statusCode': 200 if todos_validos else 400,
            'body': {
                'message': 'Validação concluída',
                'total_arquivos': len(resultados),
                'validos': sum(1 for r in resultados if r['valido']),
                'invalidos': sum(1 for r in resultados if not r['valido']),
                'resultados': resultados
            }
        }

    except Exception as e:
        logger.error(f"ERRO INESPERADO no Lambda: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }


# =============================================================================
# TESTE LOCAL
# =============================================================================
if __name__ == '__main__':
    # Simula um evento S3
    evento_teste = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "data-platform-local"},
                    "object": {
                        "key": "raw/clientes/teste/clientes_teste.json",
                        "size": 1024
                    }
                }
            }
        ]
    }

    resultado = lambda_handler(evento_teste)
    print(f"\nStatus: {resultado['statusCode']}")
    print(f"Resultado: {json.dumps(resultado['body'], indent=2, ensure_ascii=False)}")
