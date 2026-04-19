"""
archive_old_data.py - Lambda: Arquiva dados antigos automaticamente

CONCEITO AWS:
    Esta Lambda é acionada por agendamento via EventBridge (cron):
        EventBridge Rule:
            Schedule: cron(0 2 * * ? *)  # Todo dia às 02:00 UTC
            Target: Lambda archive_old_data

    Verifica arquivos em s3/raw/ com mais de 90 dias e os move para s3/archived/.
    Simula a transição automática para S3 Glacier.

    No AWS real, você configuraria isso nas Lifecycle Rules do S3:
        {
            "Rules": [{
                "ID": "ArquivarDadosBrutos",
                "Status": "Enabled",
                "Filter": {"Prefix": "raw/"},
                "Transitions": [{
                    "Days": 90,
                    "StorageClass": "GLACIER"
                }]
            }]
        }

    Mas às vezes você quer lógica mais complexa (verificar se já foi processado,
    gerar relatório, etc.), então usa Lambda.
"""

import sys
import json
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

from s3.scripts.s3_utils import S3Local

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [LAMBDA:archive_old_data] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('Lambda-ArchiveOldData')


def lambda_handler(event: Dict, context: Any = None) -> Dict:
    """
    Arquiva arquivos com mais de DIAS_PARA_ARQUIVAR dias de s3/raw/ para s3/archived/.

    Evento esperado (via EventBridge):
        {
            "dias_limite": 90,       # Padrão: 90 dias
            "prefixo": "raw/",       # Prefixo a varrer
            "dry_run": false         # Se true, não executa de verdade
        }
    """
    dias_limite = event.get('dias_limite', int(os.getenv('S3_ARCHIVE_AFTER_DAYS', 90)))
    prefixo = event.get('prefixo', 'raw/')
    dry_run = event.get('dry_run', False)

    logger.info(f"Iniciando arquivamento: {prefixo} (>{dias_limite} dias)")
    if dry_run:
        logger.info("MODO DRY-RUN: nenhum arquivo será movido")

    s3 = S3Local()
    data_limite = datetime.now() - timedelta(days=dias_limite)

    arquivos_arquivados = []
    erros = []
    total_bytes = 0

    objetos = s3.list_objects(prefix=prefixo)

    for obj in objetos:
        key = obj['Key']
        file_path = s3._key_to_path(key)

        if not file_path.exists() or not file_path.is_file():
            continue

        # Verifica idade do arquivo
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        idade_dias = (datetime.now() - mtime).days

        if mtime < data_limite:
            # Calcula o caminho no archived/
            archived_key = key.replace('raw/', 'archived/', 1)

            logger.info(f"Arquivando: {key} ({idade_dias} dias)")

            if not dry_run:
                try:
                    s3.move_object(key, archived_key)
                    arquivos_arquivados.append({
                        'original': key,
                        'archived': archived_key,
                        'idade_dias': idade_dias,
                        'tamanho_bytes': obj['Size']
                    })
                    total_bytes += obj['Size']
                except Exception as e:
                    erros.append({'key': key, 'erro': str(e)})
                    logger.error(f"Erro ao arquivar {key}: {e}")
            else:
                arquivos_arquivados.append({
                    'original': key,
                    'archived': archived_key,
                    'idade_dias': idade_dias,
                    'tamanho_bytes': obj['Size'],
                    'dry_run': True
                })
                total_bytes += obj['Size']

    logger.info(f"Arquivamento concluído: {len(arquivos_arquivados)} arquivos, "
                f"{total_bytes/1024/1024:.2f} MB")

    return {
        'statusCode': 200,
        'body': {
            'dry_run': dry_run,
            'dias_limite': dias_limite,
            'prefixo': prefixo,
            'arquivos_arquivados': len(arquivos_arquivados),
            'total_mb': round(total_bytes / 1024 / 1024, 2),
            'erros': len(erros),
            'detalhes': arquivos_arquivados[:20]  # Máximo 20 no retorno
        }
    }


if __name__ == '__main__':
    # Teste em modo dry-run (não move nenhum arquivo)
    resultado = lambda_handler({'dry_run': True, 'dias_limite': 0})
    print(f"Status: {resultado['statusCode']}")
    print(json.dumps(resultado['body'], indent=2, ensure_ascii=False))
