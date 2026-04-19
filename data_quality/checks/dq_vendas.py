"""
dq_vendas.py - Verificações de qualidade de dados para vendas
"""

import sys
import json
import logging
import io
from pathlib import Path
from datetime import datetime
from typing import Dict

import pandas as pd

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [DQ-VENDAS] %(levelname)s: %(message)s')
logger = logging.getLogger('DQ-Vendas')

CANAIS_VALIDOS = {'LOJA_FISICA', 'ECOMMERCE', 'TELEVENDAS', 'APP_MOBILE'}
STATUS_VALIDOS = {'CONCLUIDA', 'CANCELADA', 'DEVOLVIDA', 'PENDENTE'}


def carregar_vendas() -> pd.DataFrame:
    """Carrega dados de vendas do S3 local."""
    from s3.scripts.s3_utils import S3Local

    s3 = S3Local()
    objetos = s3.list_objects('raw/vendas/')

    dfs = []
    for obj in objetos:
        if not obj['Key'].endswith('.csv'):
            continue
        response = s3.get_object(obj['Key'])
        conteudo = response['Body'].decode('utf-8')
        try:
            dfs.append(pd.read_csv(io.StringIO(conteudo)))
        except Exception as e:
            logger.warning(f"Erro ao ler {obj['Key']}: {e}")

    if not dfs:
        logger.warning("Sem dados de vendas. Execute: python scripts/generate_sample_data.py")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def executar_checks_vendas(df: pd.DataFrame) -> Dict:
    """Executa checks de qualidade nas vendas."""
    if df.empty:
        return {'status': 'SEM_DADOS', 'passou': False, 'checks': []}

    total = len(df)
    checks = []
    passou_geral = True

    def check(nome, passou, valor, minimo, detalhes=""):
        nonlocal passou_geral
        if not passou:
            passou_geral = False
        checks.append({'check': nome, 'status': 'PASSOU' if passou else 'FALHOU',
                        'valor_atual': round(valor, 4), 'minimo_esperado': minimo})
        logger.info(f"  [{'OK' if passou else 'FALHOU'}] {nome}: {valor:.2%}")

    logger.info(f"Analisando {total:,} vendas")

    # Valor total não nulo e positivo
    if 'valor_total' in df.columns:
        vals = pd.to_numeric(df['valor_total'].astype(str).str.replace(r'[R$\s.,]', '',
                                                                         regex=True),
                              errors='coerce')
        taxa_valido = (vals > 0).mean()
        check("Valor total positivo", taxa_valido >= 0.99, taxa_valido, 0.99)

        # Detecta outliers extremos (> R$500.000)
        outliers = (vals > 500000).mean()
        check("Sem outliers extremos (<500k)", outliers <= 0.001, 1 - outliers, 0.999)

    # Canal de venda válido
    if 'canal_venda' in df.columns:
        taxa = df['canal_venda'].dropna().str.upper().isin(CANAIS_VALIDOS).mean()
        check("Canal de venda válido", taxa >= 0.99, taxa, 0.99)

    # Status válido
    if 'status_venda' in df.columns:
        taxa = df['status_venda'].dropna().str.upper().isin(STATUS_VALIDOS).mean()
        check("Status venda válido", taxa >= 0.99, taxa, 0.99)

    # Data de venda não nula
    if 'data_venda' in df.columns:
        taxa = 1 - df['data_venda'].isna().mean()
        check("Data venda não-nula", taxa >= 0.99, taxa, 0.99)

    # Volume mínimo
    tem_vol = total >= 1
    check("Volume mínimo (>=1)", tem_vol, 1.0 if tem_vol else 0.0, 1.0,
           f"{total:,} registros")

    return {'status': 'PASSOU' if passou_geral else 'FALHOU',
            'passou': passou_geral, 'checks': checks}


def run_dq(salvar_relatorio: bool = True) -> Dict:
    logger.info("=" * 50)
    logger.info("INICIANDO DATA QUALITY - VENDAS")
    logger.info("=" * 50)

    df = carregar_vendas()
    resultado = executar_checks_vendas(df)

    logger.info(f"\nRESULTADO: {resultado['status']}")
    passou_checks = sum(1 for c in resultado['checks'] if c['status'] == 'PASSOU')
    logger.info(f"Checks: {passou_checks}/{len(resultado['checks'])} passaram")

    if salvar_relatorio:
        relatorio_dir = projeto_root / 'data_quality' / 'relatorios'
        relatorio_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        relatorio = {**resultado, 'timestamp': datetime.now().isoformat(),
                     'dataset': 'vendas', 'total_registros': len(df)}
        with open(relatorio_dir / f'dq_vendas_{ts}.json', 'w') as f:
            json.dump(relatorio, f, ensure_ascii=False, indent=2)

    return resultado


if __name__ == '__main__':
    resultado = run_dq()
    sys.exit(0 if resultado['passou'] else 1)
