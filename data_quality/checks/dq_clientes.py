"""
dq_clientes.py - Verificações de qualidade de dados para clientes

Implementa manualmente as regras do clientes_expectations.json
usando Pandas (sem precisar instalar Great Expectations completo).

No AWS Glue Data Quality seria:
    Rules = [
        IsComplete "cpf",
        ColumnValues "uf" in ["SP","RJ","MG",...],
        Uniqueness "cpf" > 0.95
    ]
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [DQ-CLIENTES] %(levelname)s: %(message)s')
logger = logging.getLogger('DQ-Clientes')

UFS_VALIDAS = {'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS',
               'MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'}


class DQResultado:
    """Resultado de uma verificação de qualidade."""
    def __init__(self):
        self.checks: List[Dict] = []
        self.passou = True

    def adicionar_check(self, nome: str, passou: bool, valor: float,
                        minimo: float, detalhes: str = ""):
        status = "PASSOU" if passou else "FALHOU"
        if not passou:
            self.passou = False
        self.checks.append({
            'check': nome,
            'status': status,
            'valor_atual': round(valor, 4),
            'minimo_esperado': minimo,
            'detalhes': detalhes
        })
        icon = "OK" if passou else "FALHOU"
        logger.info(f"  [{icon}] {nome}: {valor:.2%} (mínimo: {minimo:.0%})")


def carregar_clientes() -> pd.DataFrame:
    """Carrega dados de clientes do S3 local."""
    from s3.scripts.s3_utils import S3Local
    import io

    s3 = S3Local()
    objetos = s3.list_objects('raw/clientes/')

    dfs = []
    for obj in objetos:
        if not obj['Key'].endswith(('.json', '.jsonl')):
            continue
        response = s3.get_object(obj['Key'])
        conteudo = response['Body'].decode('utf-8')
        try:
            dados = json.loads(conteudo)
            if isinstance(dados, list):
                dfs.append(pd.DataFrame(dados))
            elif isinstance(dados, dict):
                for v in dados.values():
                    if isinstance(v, list):
                        dfs.append(pd.DataFrame(v))
                        break
        except Exception:
            pass

    if not dfs:
        logger.warning("Sem dados de clientes. Execute: python scripts/generate_sample_data.py")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def executar_checks(df: pd.DataFrame) -> DQResultado:
    """Executa todos os checks de qualidade."""
    resultado = DQResultado()

    if df.empty:
        logger.warning("DataFrame vazio - pulando checks")
        return resultado

    total = len(df)
    logger.info(f"Analisando {total:,} registros de clientes")

    # Check 1: CPF não nulo
    taxa = 1 - df['cpf'].isna().mean() if 'cpf' in df.columns else 0
    resultado.adicionar_check("CPF não-nulo", taxa >= 0.99, taxa, 0.99)

    # Check 2: Nome não nulo
    taxa = 1 - df['nome'].isna().mean() if 'nome' in df.columns else 0
    resultado.adicionar_check("Nome não-nulo", taxa >= 0.99, taxa, 0.99)

    # Check 3: CPF com tamanho válido (11-14 caracteres)
    if 'cpf' in df.columns:
        cpfs_validos = df['cpf'].dropna().astype(str).str.len().between(11, 14)
        taxa = cpfs_validos.mean()
        resultado.adicionar_check("CPF tamanho válido", taxa >= 0.95, taxa, 0.95)

    # Check 4: Email formato válido
    if 'email' in df.columns:
        emails_validos = df['email'].dropna().astype(str).str.match(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
        taxa = emails_validos.mean() if len(emails_validos) > 0 else 0
        resultado.adicionar_check("Email formato válido", taxa >= 0.90, taxa, 0.90)

    # Check 5: UF válida
    if 'uf' in df.columns:
        ufs_validas = df['uf'].dropna().str.upper().isin(UFS_VALIDAS)
        taxa = ufs_validas.mean()
        resultado.adicionar_check("UF estado válido", taxa >= 0.98, taxa, 0.98)

    # Check 6: Unicidade de CPF
    if 'cpf' in df.columns:
        taxa = df['cpf'].nunique() / total
        resultado.adicionar_check("CPF unicidade", taxa >= 0.95, taxa, 0.95,
                                   f"{df['cpf'].nunique():,} únicos de {total:,}")

    # Check 7: Volume mínimo
    tem_minimo = total >= 10
    resultado.adicionar_check("Volume mínimo (>=10)", tem_minimo,
                               1.0 if tem_minimo else 0.0, 1.0, f"{total:,} registros")

    return resultado


def run_dq(salvar_relatorio: bool = True) -> Dict:
    """Executa DQ completa e retorna resultado."""
    logger.info("=" * 50)
    logger.info("INICIANDO DATA QUALITY - CLIENTES")
    logger.info("=" * 50)

    df = carregar_clientes()
    resultado = executar_checks(df)

    status_final = "PASSOU" if resultado.passou else "FALHOU"
    logger.info(f"\nRESULTADO: {status_final}")

    total_checks = len(resultado.checks)
    passou_checks = sum(1 for c in resultado.checks if c['status'] == 'PASSOU')
    logger.info(f"Checks: {passou_checks}/{total_checks} passaram")

    if salvar_relatorio:
        relatorio_dir = projeto_root / 'data_quality' / 'relatorios'
        relatorio_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        relatorio = {
            'timestamp': datetime.now().isoformat(),
            'dataset': 'clientes',
            'status': status_final,
            'total_registros': len(df),
            'checks': resultado.checks
        }
        with open(relatorio_dir / f'dq_clientes_{ts}.json', 'w') as f:
            json.dump(relatorio, f, ensure_ascii=False, indent=2)
        logger.info(f"Relatório salvo em data_quality/relatorios/dq_clientes_{ts}.json")

    return {'status': status_final, 'passou': resultado.passou, 'checks': resultado.checks}


if __name__ == '__main__':
    resultado = run_dq()
    sys.exit(0 if resultado['passou'] else 1)
