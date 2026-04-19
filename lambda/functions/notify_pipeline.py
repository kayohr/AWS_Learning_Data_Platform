"""
notify_pipeline.py - Lambda: Envia notificações sobre falhas no pipeline

CONCEITO AWS:
    No AWS real, este Lambda seria chamado por:
        - CloudWatch Alarms (quando métricas excedem limiares)
        - SNS (Simple Notification Service) para distribuir notificações
        - EventBridge quando um Glue Job falha

    Arquitetura de notificação AWS:
        Glue Job falhou → CloudWatch Event → EventBridge Rule
        → Lambda notify_pipeline → SNS Topic
        → Email (SES) + Slack (via HTTP endpoint) + PagerDuty

    Custo: SNS cobra $0.50/1M notificações (praticamente grátis)
"""

import sys
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import urllib.request
import urllib.error

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [LAMBDA:notify_pipeline] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('Lambda-NotifyPipeline')


# Tipos de evento que podem acionar notificação
TIPOS_EVENTO = {
    'PIPELINE_FAILURE': {
        'nivel': 'CRITICO',
        'emoji': '[ERRO]',
        'assunto': 'Pipeline FALHOU'
    },
    'DATA_QUALITY_FAILURE': {
        'nivel': 'ALTO',
        'emoji': '[QUALIDADE]',
        'assunto': 'Falha na qualidade dos dados'
    },
    'PIPELINE_SUCCESS': {
        'nivel': 'INFO',
        'emoji': '[OK]',
        'assunto': 'Pipeline concluído com sucesso'
    },
    'SCHEMA_VALIDATION_ERROR': {
        'nivel': 'ALTO',
        'emoji': '[SCHEMA]',
        'assunto': 'Erro de validação de schema'
    },
    'DATA_FRESHNESS_ALERT': {
        'nivel': 'MEDIO',
        'emoji': '[FRESHNESS]',
        'assunto': 'Dados desatualizados detectados'
    },
    'ROW_COUNT_DROP': {
        'nivel': 'ALTO',
        'emoji': '[VOLUME]',
        'assunto': 'Queda no volume de dados detectada'
    }
}


def formatar_mensagem_email(tipo_evento: str, detalhes: Dict) -> str:
    """
    Formata a mensagem de notificação por e-mail.

    No AWS: seria enviado via SES (Simple Email Service)
        ses_client.send_email(
            Source='alertas@empresa.com',
            Destination={'ToAddresses': ['equipe-dados@empresa.com']},
            Message={
                'Subject': {'Data': assunto},
                'Body': {'Text': {'Data': mensagem}}
            }
        )
    """
    config = TIPOS_EVENTO.get(tipo_evento, {})
    nivel = config.get('nivel', 'INFO')
    assunto = config.get('assunto', tipo_evento)

    pipeline_nome = detalhes.get('pipeline_nome', 'desconhecido')
    mensagem_erro = detalhes.get('mensagem_erro', 'Sem detalhes')
    timestamp = detalhes.get('timestamp', datetime.now().isoformat())
    ambiente = detalhes.get('ambiente', 'development')

    mensagem = f"""
========================================
ALERTA DA PLATAFORMA DE DADOS - {nivel}
========================================

Evento: {assunto}
Pipeline: {pipeline_nome}
Ambiente: {ambiente.upper()}
Horário: {timestamp}

DETALHES:
{mensagem_erro}

---
Componente: {detalhes.get('componente', 'N/A')}
Tentativa: {detalhes.get('tentativa', 1)} de {detalhes.get('max_tentativas', 3)}

Registros afetados: {detalhes.get('registros_afetados', 'N/A')}
Último arquivo processado: {detalhes.get('ultimo_arquivo', 'N/A')}

---
Para investigar, acesse:
  Airflow UI: http://localhost:8080
  Logs: {detalhes.get('log_path', 'N/A')}

Este é um alerta automático da Plataforma de Dados.
========================================
"""
    return mensagem


def formatar_mensagem_slack(tipo_evento: str, detalhes: Dict) -> Dict:
    """
    Formata mensagem para Slack (formato Block Kit da API Slack).

    No AWS: o Lambda faria uma chamada HTTP para o webhook do Slack.
    """
    config = TIPOS_EVENTO.get(tipo_evento, {})
    emoji = config.get('emoji', '[ALERTA]')
    assunto = config.get('assunto', tipo_evento)
    nivel = config.get('nivel', 'INFO')

    # Mapa de cores por nível (Slack usa cores em hex nos attachments)
    cores = {
        'CRITICO': '#FF0000',  # Vermelho
        'ALTO': '#FF6600',     # Laranja
        'MEDIO': '#FFCC00',    # Amarelo
        'INFO': '#00CC00'      # Verde
    }

    return {
        "text": f"{emoji} *{assunto}*",
        "attachments": [
            {
                "color": cores.get(nivel, '#808080'),
                "fields": [
                    {
                        "title": "Pipeline",
                        "value": detalhes.get('pipeline_nome', 'N/A'),
                        "short": True
                    },
                    {
                        "title": "Ambiente",
                        "value": detalhes.get('ambiente', 'development').upper(),
                        "short": True
                    },
                    {
                        "title": "Horário",
                        "value": detalhes.get('timestamp', datetime.now().isoformat()),
                        "short": True
                    },
                    {
                        "title": "Nível",
                        "value": nivel,
                        "short": True
                    },
                    {
                        "title": "Detalhes",
                        "value": detalhes.get('mensagem_erro', 'Sem detalhes')[:300],
                        "short": False
                    }
                ],
                "footer": "Data Platform Alerts",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }


def enviar_notificacao_slack(webhook_url: str, mensagem: Dict) -> bool:
    """
    Envia notificação para o Slack via webhook.

    No AWS Lambda real: o mesmo código funcionaria,
    pois Lambda tem acesso à internet via NAT Gateway.

    Returns:
        True se enviado com sucesso
    """
    if not webhook_url or 'FAKE' in webhook_url or 'localhost' in webhook_url:
        logger.info("  [SIMULAÇÃO] Notificação Slack seria enviada:")
        logger.info(f"  {json.dumps(mensagem, ensure_ascii=False, indent=2)[:300]}")
        return True

    try:
        dados = json.dumps(mensagem).encode('utf-8')
        req = urllib.request.Request(
            webhook_url,
            data=dados,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status == 200
    except Exception as e:
        logger.error(f"Erro ao enviar para Slack: {e}")
        return False


def salvar_notificacao_local(tipo_evento: str, detalhes: Dict,
                              mensagem: str) -> str:
    """
    Salva a notificação em arquivo local (simula envio por email/SNS).

    No AWS: seria enviado via SES (email) ou SNS (push/SMS/email).
    """
    notificacoes_dir = projeto_root / 'monitoring' / 'notificacoes'
    notificacoes_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    arquivo = notificacoes_dir / f"notif_{tipo_evento}_{timestamp}.json"

    registro = {
        'tipo_evento': tipo_evento,
        'timestamp': datetime.now().isoformat(),
        'detalhes': detalhes,
        'mensagem_formatada': mensagem,
        'enviado_para': {
            'email': detalhes.get('email_destino', os.getenv('ALERT_EMAIL', 'N/A')),
            'slack': 'simulado',
            'sns': 'simulado'
        }
    }

    with open(arquivo, 'w', encoding='utf-8') as f:
        json.dump(registro, f, ensure_ascii=False, indent=2)

    logger.info(f"Notificação salva: {arquivo}")
    return str(arquivo)


def lambda_handler(event: Dict, context: Any = None) -> Dict:
    """
    Handler da Lambda de notificação.

    Eventos esperados:
        - Direto: {'tipo_evento': 'PIPELINE_FAILURE', 'detalhes': {...}}
        - Via SNS: {'Records': [{'Sns': {'Message': '{"tipo_evento": ...}'}}]}
        - Via CloudWatch: {'source': 'aws.glue', 'detail': {...}}

    Args:
        event: Evento com tipo e detalhes do alerta
        context: Contexto Lambda (timeout restante, etc.)

    Returns:
        Status da notificação
    """
    logger.info("Lambda notify_pipeline invocado")

    # Extrai tipo e detalhes do evento
    tipo_evento = event.get('tipo_evento', 'PIPELINE_FAILURE')
    detalhes = event.get('detalhes', {})

    # Adiciona timestamp se não tiver
    if 'timestamp' not in detalhes:
        detalhes['timestamp'] = datetime.now().isoformat()

    # Adiciona ambiente se não tiver
    if 'ambiente' not in detalhes:
        detalhes['ambiente'] = os.getenv('ENVIRONMENT', 'development')

    logger.info(f"Tipo de evento: {tipo_evento}")
    logger.info(f"Pipeline: {detalhes.get('pipeline_nome', 'N/A')}")
    logger.info(f"Nível: {TIPOS_EVENTO.get(tipo_evento, {}).get('nivel', 'INFO')}")

    notificacoes_enviadas = []
    erros = []

    try:
        # 1. Formata a mensagem de e-mail
        mensagem_email = formatar_mensagem_email(tipo_evento, detalhes)

        # 2. Formata mensagem Slack
        mensagem_slack = formatar_mensagem_slack(tipo_evento, detalhes)

        # 3. Envia para Slack (se webhook configurado)
        slack_webhook = os.getenv('SLACK_WEBHOOK_URL', '')
        slack_ok = enviar_notificacao_slack(slack_webhook, mensagem_slack)
        if slack_ok:
            notificacoes_enviadas.append('slack')

        # 4. Salva localmente (simula e-mail/SNS)
        arquivo = salvar_notificacao_local(tipo_evento, detalhes, mensagem_email)
        notificacoes_enviadas.append('arquivo_local')

        logger.info(f"Notificações enviadas: {notificacoes_enviadas}")

        return {
            'statusCode': 200,
            'body': {
                'message': 'Notificações enviadas',
                'tipo_evento': tipo_evento,
                'notificacoes': notificacoes_enviadas,
                'arquivo_log': arquivo
            }
        }

    except Exception as e:
        logger.error(f"Erro ao enviar notificação: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }


# =============================================================================
# FUNÇÕES AUXILIARES PARA CHAMAR ESTE LAMBDA
# =============================================================================
def notificar_falha_pipeline(pipeline_nome: str, mensagem_erro: str,
                              componente: str = None) -> Dict:
    """
    Atalho para notificar falha de pipeline.

    Uso no Airflow (on_failure_callback):
        from lambda.functions.notify_pipeline import notificar_falha_pipeline
        notificar_falha_pipeline('dag_etl_full_pipeline', str(context['exception']))
    """
    return lambda_handler({
        'tipo_evento': 'PIPELINE_FAILURE',
        'detalhes': {
            'pipeline_nome': pipeline_nome,
            'mensagem_erro': mensagem_erro,
            'componente': componente,
            'timestamp': datetime.now().isoformat()
        }
    })


def notificar_sucesso_pipeline(pipeline_nome: str,
                                registros_processados: int = 0) -> Dict:
    """Atalho para notificar sucesso de pipeline."""
    return lambda_handler({
        'tipo_evento': 'PIPELINE_SUCCESS',
        'detalhes': {
            'pipeline_nome': pipeline_nome,
            'mensagem_erro': f'Pipeline concluído. Registros: {registros_processados}',
            'registros_afetados': registros_processados,
            'timestamp': datetime.now().isoformat()
        }
    })


if __name__ == '__main__':
    # Testa notificação de falha
    resultado = notificar_falha_pipeline(
        pipeline_nome='dag_etl_full_pipeline',
        mensagem_erro='Job Glue falhou na transformação de clientes: KeyError "cpf"',
        componente='glue_job_clientes'
    )
    print(f"Status: {resultado['statusCode']}")
    print(f"Resultado: {json.dumps(resultado['body'], indent=2, ensure_ascii=False)}")
