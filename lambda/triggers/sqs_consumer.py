"""
sqs_consumer.py - Simula o trigger SQS → Lambda

CONCEITO AWS:
    No AWS, você pode configurar uma fila SQS como trigger do Lambda:
        - Lambda polling: Lambda verifica a fila a cada poucos segundos
        - Quando há mensagens, Lambda é invocado com batch (1-10000 msgs)
        - Se Lambda retornar erro: mensagens voltam para a fila (retry)
        - Após X tentativas: mensagem vai para DLQ (Dead Letter Queue)

    Configuração no AWS:
        Lambda → Add Trigger → SQS
        Queue ARN: arn:aws:sqs:us-east-1:123456789:vendas-queue
        Batch size: 10 (processa 10 mensagens por vez)
        Max concurrency: 5 (máximo 5 lambdas simultâneos)
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Callable

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

from sqs.sqs_local import SQSLocal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [SQS-TRIGGER] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('SQSConsumer')


def processar_mensagem_venda(mensagem: Dict) -> Dict:
    """
    Processa uma mensagem de venda da fila SQS.
    Esta seria uma função Lambda separada no AWS.
    """
    logger.info(f"Processando venda: {mensagem.get('id_venda', 'N/A')}")
    # Lógica de processamento...
    return {'status': 'processed', 'id': mensagem.get('id_venda')}


class SQSLambdaTrigger:
    """Simula o polling SQS do Lambda AWS."""

    def __init__(self, queue_name: str, batch_size: int = 10):
        self.sqs = SQSLocal()
        self.queue_name = queue_name
        self.batch_size = batch_size
        self.handlers: List[Callable] = []

    def registrar_handler(self, func: Callable):
        self.handlers.append(func)

    def processar_batch(self) -> int:
        """Processa um batch de mensagens."""
        mensagens = self.sqs.receive_messages(
            self.queue_name, max_messages=self.batch_size
        )

        if not mensagens:
            return 0

        logger.info(f"Batch recebido: {len(mensagens)} mensagens")

        processadas = 0
        for msg in mensagens:
            try:
                evento = {
                    'Records': [{'body': msg['Body'],
                                 'messageId': msg['MessageId'],
                                 'receiptHandle': msg['ReceiptHandle']}]
                }
                for handler in self.handlers:
                    handler(evento)

                self.sqs.delete_message(self.queue_name, msg['ReceiptHandle'])
                processadas += 1

            except Exception as e:
                logger.error(f"Erro ao processar mensagem: {e}")
                # No AWS: mensagem volta para a fila e vai para DLQ após N tentativas

        return processadas


if __name__ == '__main__':
    trigger = SQSLambdaTrigger('vendas_queue', batch_size=5)
    processadas = trigger.processar_batch()
    logger.info(f"Mensagens processadas: {processadas}")
