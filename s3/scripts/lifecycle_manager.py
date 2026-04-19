"""
lifecycle_manager.py - Simula as Lifecycle Policies do Amazon S3

CONCEITO AWS S3 LIFECYCLE:
    No Amazon S3 real, você pode configurar regras automáticas para gerenciar
    objetos ao longo do tempo:

    Regra exemplo na AWS:
    {
        "Rules": [{
            "ID": "MoverParaGlacier",
            "Status": "Enabled",
            "Filter": {"Prefix": "raw/"},
            "Transitions": [
                {"Days": 30, "StorageClass": "STANDARD_IA"},
                {"Days": 90, "StorageClass": "GLACIER"},
                {"Days": 365, "StorageClass": "DEEP_ARCHIVE"}
            ],
            "Expiration": {"Days": 730}  # Deletar após 2 anos
        }]
    }

    Aqui simulamos esse comportamento com Python:
    - Verificamos a data de criação dos arquivos
    - Movemos para a pasta 'archived/' quando passam do limite
    - Deletamos quando passam do limite de retenção total

USO:
    python lifecycle_manager.py               # Aplica regras com configuração padrão
    python lifecycle_manager.py --dry-run     # Simula sem mover/deletar
    python lifecycle_manager.py --verbose     # Mostra todos os arquivos avaliados
"""

import os
import json
import shutil
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from dataclasses import dataclass, field

# Importa o cliente S3 local
import sys
sys.path.insert(0, str(Path(__file__).parent))
from s3_utils import S3Local

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('LifecycleManager')


@dataclass
class LifecycleRule:
    """
    Define uma regra de ciclo de vida para objetos S3.

    No AWS real, isso seria configurado via console ou CloudFormation.
    Aqui representamos como um objeto Python.
    """
    rule_id: str               # Nome da regra
    prefix: str                # Qual prefixo aplicar (ex: 'raw/')
    archive_after_days: int    # Mover para 'archived/' após N dias
    delete_after_days: int     # Deletar da 'archived/' após M dias
    enabled: bool = True       # Se a regra está ativa
    description: str = ""      # Descrição para documentação


# =============================================================================
# CONFIGURAÇÃO DAS REGRAS DE LIFECYCLE
# =============================================================================
# No AWS real, essas regras são configuradas via:
#   - Console AWS → S3 → Bucket → Management → Lifecycle rules
#   - AWS CLI: aws s3api put-bucket-lifecycle-configuration
#   - Terraform/CloudFormation
# =============================================================================
DEFAULT_LIFECYCLE_RULES = [
    LifecycleRule(
        rule_id="raw-dados-brutos",
        prefix="raw/",
        archive_after_days=90,    # Arquivos raw ficam 90 dias em standard
        delete_after_days=730,    # Deletados após 2 anos no archive
        description="Dados brutos da ingestão - alta frequência de acesso inicial"
    ),
    LifecycleRule(
        rule_id="processed-dados-processados",
        prefix="processed/",
        archive_after_days=180,   # Processados ficam 6 meses em standard
        delete_after_days=1095,   # Deletados após 3 anos no archive
        description="Dados processados - menor frequência de acesso"
    ),
]


class LifecycleManager:
    """
    Gerencia o ciclo de vida dos arquivos no S3 local.

    No AWS real, a AWS executa isso automaticamente.
    Aqui, precisamos rodar este script periodicamente (via Airflow/cron).

    No AWS, o equivalente ao Airflow que agenda isso seria:
        - EventBridge (CloudWatch Events) → Lambda → executa a lógica
        - Ou simplesmente configurar as Lifecycle Rules no próprio S3
    """

    def __init__(self, base_path: str = None, rules: List[LifecycleRule] = None):
        """
        Inicializa o gerenciador de lifecycle.

        Args:
            base_path: Caminho base do S3 local
            rules: Lista de regras de lifecycle a aplicar
        """
        self.s3 = S3Local(base_path)
        self.rules = rules or DEFAULT_LIFECYCLE_RULES

        # Estatísticas de execução
        self.stats = {
            'arquivos_avaliados': 0,
            'arquivos_arquivados': 0,
            'arquivos_deletados': 0,
            'bytes_movidos': 0,
            'bytes_deletados': 0,
            'erros': 0
        }

    def _get_file_age_days(self, file_path: Path) -> int:
        """
        Retorna a idade do arquivo em dias.

        No S3 real, usamos a data de criação do objeto (s3.head_object → LastModified).
        Localmente, usamos a data de modificação do arquivo.
        """
        mtime = file_path.stat().st_mtime
        age = datetime.now() - datetime.fromtimestamp(mtime)
        return age.days

    def apply_rules(self, dry_run: bool = False, verbose: bool = False) -> Dict:
        """
        Aplica todas as regras de lifecycle configuradas.

        Args:
            dry_run: Se True, apenas mostra o que seria feito (não executa)
            verbose: Se True, mostra todos os arquivos avaliados

        Returns:
            Dicionário com estatísticas da execução
        """
        logger.info("=" * 60)
        logger.info("INICIANDO GERENCIAMENTO DE LIFECYCLE S3")
        if dry_run:
            logger.info("MODO DRY-RUN: Nenhuma alteração será feita")
        logger.info("=" * 60)

        inicio = datetime.now()

        for rule in self.rules:
            if not rule.enabled:
                logger.info(f"Regra '{rule.rule_id}' está DESABILITADA, pulando...")
                continue

            logger.info(f"\nAplicando regra: {rule.rule_id}")
            logger.info(f"  Prefixo: {rule.prefix}")
            logger.info(f"  Arquivar após: {rule.archive_after_days} dias")
            logger.info(f"  Deletar após: {rule.archive_after_days + rule.delete_after_days} dias total")

            self._apply_single_rule(rule, dry_run, verbose)

        duracao = (datetime.now() - inicio).total_seconds()

        # Log do resumo
        logger.info("\n" + "=" * 60)
        logger.info("RESUMO DO LIFECYCLE:")
        logger.info(f"  Arquivos avaliados : {self.stats['arquivos_avaliados']}")
        logger.info(f"  Arquivos arquivados: {self.stats['arquivos_arquivados']}")
        logger.info(f"  Arquivos deletados : {self.stats['arquivos_deletados']}")
        logger.info(f"  Dados movidos      : {self.stats['bytes_movidos'] / 1024 / 1024:.2f} MB")
        logger.info(f"  Dados deletados    : {self.stats['bytes_deletados'] / 1024 / 1024:.2f} MB")
        logger.info(f"  Erros              : {self.stats['erros']}")
        logger.info(f"  Duração            : {duracao:.2f} segundos")
        logger.info("=" * 60)

        # Salva relatório de execução (no AWS seria CloudWatch Logs)
        self._save_execution_report(dry_run, duracao)

        return self.stats

    def _apply_single_rule(self, rule: LifecycleRule,
                           dry_run: bool, verbose: bool):
        """
        Aplica uma regra de lifecycle específica.

        Lógica:
            1. Lista todos os arquivos no prefixo da regra
            2. Para cada arquivo, verifica a idade
            3. Se idade > archive_after_days → move para archived/
            4. Se idade > archive_after_days + delete_after_days → deleta
        """
        objetos = self.s3.list_objects(prefix=rule.prefix, max_keys=10000)

        for obj in objetos:
            key = obj['Key']
            file_path = self.s3._key_to_path(key)

            if not file_path.exists() or file_path.is_dir():
                continue

            # Calcula idade do arquivo
            idade_dias = self._get_file_age_days(file_path)
            tamanho = obj['Size']

            self.stats['arquivos_avaliados'] += 1

            if verbose:
                logger.info(f"  Avaliando: {key} ({idade_dias} dias, {tamanho} bytes)")

            # Limite total para deletar
            limite_delete = rule.archive_after_days + rule.delete_after_days

            if idade_dias >= limite_delete:
                # AÇÃO: Deletar definitivamente
                logger.info(f"  DELETAR: {key} ({idade_dias} dias >= {limite_delete})")
                if not dry_run:
                    try:
                        self.s3.delete_object(key)
                        self.stats['arquivos_deletados'] += 1
                        self.stats['bytes_deletados'] += tamanho
                    except Exception as e:
                        logger.error(f"  ERRO ao deletar {key}: {e}")
                        self.stats['erros'] += 1

            elif idade_dias >= rule.archive_after_days:
                # AÇÃO: Mover para archived/ (simula transição para S3 Glacier)
                archived_key = key.replace(rule.prefix, 'archived/', 1)
                logger.info(f"  ARQUIVAR: {key} → {archived_key} ({idade_dias} dias)")
                if not dry_run:
                    try:
                        self.s3.move_object(key, archived_key)
                        self.stats['arquivos_arquivados'] += 1
                        self.stats['bytes_movidos'] += tamanho
                    except Exception as e:
                        logger.error(f"  ERRO ao arquivar {key}: {e}")
                        self.stats['erros'] += 1

    def _save_execution_report(self, dry_run: bool, duracao: float):
        """
        Salva relatório de execução do lifecycle.

        No AWS real, isso seria:
            - Logs no CloudWatch
            - Métricas no CloudWatch Metrics
            - Relatório S3 Storage Lens
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'dry_run': dry_run,
            'duracao_segundos': duracao,
            'stats': self.stats,
            'regras_aplicadas': [
                {
                    'id': r.rule_id,
                    'prefix': r.prefix,
                    'archive_after_days': r.archive_after_days,
                    'delete_after_days': r.delete_after_days
                }
                for r in self.rules if r.enabled
            ]
        }

        # Salva o relatório na pasta de monitoring
        report_path = self.s3.base_path.parent / 'monitoring' / 'lifecycle_reports'
        report_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = report_path / f'lifecycle_report_{timestamp}.json'

        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"\nRelatório salvo em: {report_file}")

    def get_files_pending_action(self) -> Dict[str, List[str]]:
        """
        Retorna arquivos que estão próximos de serem arquivados/deletados.
        Útil para dashboard de monitoramento.
        """
        pendentes = {'arquivar_em_breve': [], 'deletar_em_breve': []}

        for rule in self.rules:
            if not rule.enabled:
                continue

            objetos = self.s3.list_objects(prefix=rule.prefix)

            for obj in objetos:
                key = obj['Key']
                file_path = self.s3._key_to_path(key)

                if not file_path.exists():
                    continue

                idade_dias = self._get_file_age_days(file_path)
                dias_para_arquivar = rule.archive_after_days - idade_dias
                limite_delete = rule.archive_after_days + rule.delete_after_days
                dias_para_deletar = limite_delete - idade_dias

                # Alerta se faltam menos de 7 dias para arquivar
                if 0 < dias_para_arquivar <= 7:
                    pendentes['arquivar_em_breve'].append(
                        f"{key} (em {dias_para_arquivar} dias)"
                    )

                # Alerta se faltam menos de 7 dias para deletar
                if 0 < dias_para_deletar <= 7:
                    pendentes['deletar_em_breve'].append(
                        f"{key} (em {dias_para_deletar} dias)"
                    )

        return pendentes


# =============================================================================
# PONTO DE ENTRADA - Execução via linha de comando
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description='Gerenciador de Lifecycle do S3 Local (simula AWS S3 Lifecycle)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simula as ações sem executar (recomendado para testar)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Mostra todos os arquivos avaliados'
    )
    parser.add_argument(
        '--check-pending',
        action='store_true',
        help='Mostra arquivos próximos de ação (sem executar)'
    )

    args = parser.parse_args()
    manager = LifecycleManager()

    if args.check_pending:
        pendentes = manager.get_files_pending_action()
        print("\nArquivos próximos de ação:")
        print("  Arquivar em breve:")
        for f in pendentes['arquivar_em_breve']:
            print(f"    - {f}")
        print("  Deletar em breve:")
        for f in pendentes['deletar_em_breve']:
            print(f"    - {f}")
    else:
        manager.apply_rules(dry_run=args.dry_run, verbose=args.verbose)


if __name__ == '__main__':
    main()
