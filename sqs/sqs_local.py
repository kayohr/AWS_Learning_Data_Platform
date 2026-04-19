"""
sqs_local.py - Simulação local do Amazon SQS

CONCEITO AWS:
    O Amazon SQS é um serviço de filas totalmente gerenciado.
    Você nunca precisa gerenciar servidores ou a infraestrutura da fila.

    AWS boto3 API:
        sqs = boto3.client('sqs')
        sqs.send_message(QueueUrl='...', MessageBody='...')
        sqs.receive_message(QueueUrl='...', MaxNumberOfMessages=10)
        sqs.delete_message(QueueUrl='...', ReceiptHandle='...')

    Aqui implementamos a mesma interface usando arquivos JSON como armazenamento.
    Em produção real, você trocaria este arquivo por boto3 e as chamadas funcionariam.

ARMAZENAMENTO LOCAL:
    Cada fila é um arquivo JSON em sqs/data/{queue_name}.json
    Formato:
    {
        "queue_name": "vendas_queue",
        "messages": [
            {
                "MessageId": "uuid",
                "Body": "...",
                "ReceiptHandle": "...",
                "SentTimestamp": "...",
                "ReceiveCount": 0,
                "FirstReceiveTimestamp": null,
                "VisibilityDeadline": null
            }
        ],
        "dead_letters": [...]
    }
"""

import os
import sys
import json
import uuid
import fcntl
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [SQS-Local] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('SQSLocal')

# Pasta onde ficam os dados das filas
projeto_root = Path(__file__).parent.parent
SQS_DATA_DIR = Path(__file__).parent / 'data'
SQS_DATA_DIR.mkdir(exist_ok=True)

# Configurações padrão (lidas do arquivo de configuração)
DEFAULT_VISIBILITY_TIMEOUT = 30   # segundos
DEFAULT_MAX_RECEIVE_COUNT = 3     # tentativas antes de ir para DLQ
DEFAULT_RETENTION_PERIOD = 86400  # 24 horas em segundos


class SQSLocal:
    """
    Simula o cliente SQS da AWS usando arquivos JSON.

    Interface similar ao boto3:
        aws_sqs = boto3.client('sqs')
        local_sqs = SQSLocal()

        # Ambos suportam as mesmas operações:
        aws_sqs.send_message(QueueUrl=url, MessageBody=body)
        local_sqs.send_message('nome_fila', body)

    No AWS real, cada fila tem uma URL única:
        https://sqs.us-east-1.amazonaws.com/123456789/nome-da-fila

    Localmente, usamos apenas o nome da fila.
    """

    def __init__(self, data_dir: str = None):
        """
        Inicializa o cliente SQS local.

        Args:
            data_dir: Pasta para armazenar os dados das filas.
                     Padrão: sqs/data/
        """
        self.data_dir = Path(data_dir) if data_dir else SQS_DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Carrega configurações das filas
        self.config_filas = self._carregar_configs()

        logger.debug(f"SQSLocal iniciado. Dados em: {self.data_dir}")

    def _carregar_configs(self) -> Dict:
        """Carrega configurações das filas dos arquivos JSON."""
        configs = {}
        config_dir = Path(__file__).parent / 'queues'

        if config_dir.exists():
            for config_file in config_dir.glob('*.json'):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    if 'QueueName' in config:
                        # Remove hífens do nome (SQS permite, mas simplificamos)
                        nome = config['QueueName'].replace('-', '_')
                        configs[nome] = config

        return configs

    def _get_queue_file(self, queue_name: str) -> Path:
        """Retorna o caminho do arquivo de dados de uma fila."""
        nome_seguro = queue_name.replace('-', '_').replace('/', '_')
        return self.data_dir / f"{nome_seguro}.json"

    def _ler_fila(self, queue_name: str) -> Dict:
        """
        Lê o estado atual de uma fila do arquivo JSON.
        Thread-safe usando fcntl (file locking).
        """
        queue_file = self._get_queue_file(queue_name)

        if not queue_file.exists():
            # Cria fila vazia
            return {
                'queue_name': queue_name,
                'created_at': datetime.now().isoformat(),
                'messages': [],
                'dead_letters': [],
                'stats': {
                    'total_sent': 0,
                    'total_received': 0,
                    'total_deleted': 0,
                    'total_dlq': 0
                }
            }

        with open(queue_file, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Arquivo de fila corrompido: {queue_file}")
                return {'queue_name': queue_name, 'messages': [], 'dead_letters': [],
                        'stats': {'total_sent': 0, 'total_received': 0,
                                  'total_deleted': 0, 'total_dlq': 0}}

    def _salvar_fila(self, queue_name: str, dados: Dict):
        """Salva o estado atual de uma fila no arquivo JSON."""
        queue_file = self._get_queue_file(queue_name)

        with open(queue_file, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=2, default=str)

    def _gerar_receipt_handle(self, message_id: str) -> str:
        """
        Gera um ReceiptHandle único para uma mensagem.

        No SQS real, o ReceiptHandle é um token opaco que você usa para:
        - Confirmar que processou a mensagem (delete_message)
        - Estender o visibility timeout (change_message_visibility)

        Uma vez que o visibility timeout expira, o ReceiptHandle fica inválido
        e uma nova leitura gera um novo ReceiptHandle.
        """
        timestamp = datetime.now().isoformat()
        raw = f"{message_id}:{timestamp}:{uuid.uuid4()}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _processar_visibility_timeout(self, mensagens: List[Dict]) -> List[Dict]:
        """
        Remove mensagens que estão dentro do visibility timeout.
        Mensagens invisíveis não devem ser retornadas para outros consumers.
        """
        agora = datetime.now()
        visiveis = []

        for msg in mensagens:
            deadline = msg.get('VisibilityDeadline')
            if deadline:
                deadline_dt = datetime.fromisoformat(deadline)
                if agora < deadline_dt:
                    # Ainda dentro do visibility timeout - não visível
                    continue
                else:
                    # Visibility timeout expirou - mensagem volta para a fila
                    msg['VisibilityDeadline'] = None
                    logger.debug(f"Visibility timeout expirou para: {msg['MessageId'][:8]}...")

            visiveis.append(msg)

        return visiveis

    def send_message(self, queue_name: str,
                     message_body: Any,
                     delay_seconds: int = 0,
                     message_attributes: Dict = None) -> Dict:
        """
        Envia uma mensagem para a fila.

        AWS boto3 equivalente:
            response = sqs.send_message(
                QueueUrl='https://sqs.us-east-1.amazonaws.com/123/vendas-queue',
                MessageBody=json.dumps({'id_venda': 123}),
                DelaySeconds=0,
                MessageAttributes={
                    'fonte': {
                        'StringValue': 'sistema-ecommerce',
                        'DataType': 'String'
                    }
                }
            )
            message_id = response['MessageId']

        Args:
            queue_name: Nome da fila
            message_body: Conteúdo da mensagem (dict ou string)
            delay_seconds: Segundos para a mensagem ficar invisível (delay)
            message_attributes: Atributos extras da mensagem

        Returns:
            Dict com MessageId e MD5OfMessageBody
        """
        # Serializa para JSON se for dict
        if isinstance(message_body, dict):
            body_str = json.dumps(message_body, ensure_ascii=False)
        else:
            body_str = str(message_body)

        message_id = str(uuid.uuid4())

        # Calcula quando a mensagem fica visível (delay)
        visible_from = None
        if delay_seconds > 0:
            visible_from = (datetime.now() + timedelta(seconds=delay_seconds)).isoformat()

        mensagem = {
            'MessageId': message_id,
            'Body': body_str,
            'ReceiptHandle': None,  # Gerado quando a mensagem é lida
            'SentTimestamp': datetime.now().isoformat(),
            'ReceiveCount': 0,
            'FirstReceiveTimestamp': None,
            'VisibilityDeadline': visible_from,
            'MessageAttributes': message_attributes or {}
        }

        # Salva na fila
        dados_fila = self._ler_fila(queue_name)
        dados_fila['messages'].append(mensagem)
        dados_fila.setdefault('stats', {})
        dados_fila['stats']['total_sent'] = \
            dados_fila['stats'].get('total_sent', 0) + 1
        self._salvar_fila(queue_name, dados_fila)

        logger.info(f"Mensagem enviada para '{queue_name}': {message_id[:8]}...")

        return {
            'MessageId': message_id,
            'MD5OfMessageBody': hashlib.md5(body_str.encode()).hexdigest()
        }

    def send_message_batch(self, queue_name: str,
                           messages: List[Dict]) -> Dict:
        """
        Envia múltiplas mensagens em lote (mais eficiente).

        No SQS real: máximo de 10 mensagens por batch.
        Custo: 1 request para 10 mensagens vs 10 requests individuais.

        AWS boto3 equivalente:
            sqs.send_message_batch(
                QueueUrl=queue_url,
                Entries=[
                    {'Id': '1', 'MessageBody': json.dumps(msg1)},
                    {'Id': '2', 'MessageBody': json.dumps(msg2)},
                ]
            )
        """
        enviadas = []
        falhas = []

        for i, msg in enumerate(messages[:10]):  # SQS limita 10 por batch
            try:
                resultado = self.send_message(
                    queue_name,
                    msg.get('MessageBody', msg),
                    msg.get('DelaySeconds', 0)
                )
                enviadas.append({'Id': str(i), 'MessageId': resultado['MessageId']})
            except Exception as e:
                falhas.append({'Id': str(i), 'Code': 'Sender Fault', 'Message': str(e)})

        return {'Successful': enviadas, 'Failed': falhas}

    def receive_messages(self, queue_name: str,
                         max_messages: int = 1,
                         wait_time_seconds: int = 0,
                         visibility_timeout: int = None) -> List[Dict]:
        """
        Recebe mensagens da fila.

        IMPORTANTE: Após receber, a mensagem fica "invisível" por visibility_timeout
        segundos. Você DEVE confirmar o processamento deletando a mensagem.

        AWS boto3 equivalente:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,          # 1-10
                WaitTimeSeconds=20,              # Long polling
                VisibilityTimeout=30,
                MessageAttributeNames=['All']
            )
            messages = response.get('Messages', [])

        Args:
            queue_name: Nome da fila
            max_messages: Máximo de mensagens (1-10 no SQS real)
            wait_time_seconds: Long polling timeout (0 = short polling)
            visibility_timeout: Override do visibility timeout da fila

        Returns:
            Lista de mensagens disponíveis
        """
        # Limita a 10 (limite do SQS)
        max_messages = min(max_messages, 10)

        # Configuração da fila
        config_fila = self.config_filas.get(queue_name, {})
        vis_timeout = visibility_timeout or int(
            config_fila.get('Attributes', {}).get('VisibilityTimeout',
                                                    DEFAULT_VISIBILITY_TIMEOUT)
        )

        dados_fila = self._ler_fila(queue_name)
        mensagens = dados_fila.get('messages', [])

        # Filtra mensagens visíveis (fora do visibility timeout)
        visiveis = self._processar_visibility_timeout(mensagens)

        if not visiveis:
            logger.debug(f"Fila '{queue_name}' vazia ou todas invisíveis")
            return []

        # Seleciona as mensagens a retornar
        selecionadas = visiveis[:max_messages]
        recebidas = []

        for msg in selecionadas:
            # Gera novo ReceiptHandle
            receipt_handle = self._gerar_receipt_handle(msg['MessageId'])
            msg['ReceiptHandle'] = receipt_handle

            # Incrementa contador de recebimentos
            msg['ReceiveCount'] = msg.get('ReceiveCount', 0) + 1

            if not msg.get('FirstReceiveTimestamp'):
                msg['FirstReceiveTimestamp'] = datetime.now().isoformat()

            # Define novo visibility timeout
            msg['VisibilityDeadline'] = (
                datetime.now() + timedelta(seconds=vis_timeout)
            ).isoformat()

            # Verifica se deve ir para DLQ
            max_receive = int(
                config_fila.get('Attributes', {}).get(
                    'RedrivePolicy', {}).get('maxReceiveCount',
                                             DEFAULT_MAX_RECEIVE_COUNT)
            )

            if msg['ReceiveCount'] > max_receive:
                # Move para DLQ
                logger.warning(
                    f"Mensagem {msg['MessageId'][:8]} excedeu {max_receive} "
                    f"tentativas → DLQ"
                )
                self._mover_para_dlq(queue_name, msg, dados_fila)
                continue

            recebidas.append({
                'MessageId': msg['MessageId'],
                'ReceiptHandle': receipt_handle,
                'Body': msg['Body'],
                'Attributes': {
                    'SentTimestamp': msg.get('SentTimestamp', ''),
                    'ApproximateReceiveCount': str(msg['ReceiveCount']),
                    'ApproximateFirstReceiveTimestamp': msg.get('FirstReceiveTimestamp', '')
                },
                'MessageAttributes': msg.get('MessageAttributes', {})
            })

        # Atualiza arquivo da fila
        self._salvar_fila(queue_name, dados_fila)
        dados_fila['stats']['total_received'] = \
            dados_fila['stats'].get('total_received', 0) + len(recebidas)
        self._salvar_fila(queue_name, dados_fila)

        if recebidas:
            logger.info(f"Recebidas {len(recebidas)} mensagens de '{queue_name}'")

        return recebidas

    def delete_message(self, queue_name: str, receipt_handle: str) -> bool:
        """
        Confirma que uma mensagem foi processada (deleta da fila).

        IMPORTANTE: Você DEVE chamar delete_message após processar com sucesso.
        Se não chamar, a mensagem reaparecerá após o visibility timeout.

        AWS boto3 equivalente:
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle='AQEBwJnKyrh...'  # ReceiptHandle recebido
            )
        """
        dados_fila = self._ler_fila(queue_name)
        mensagens = dados_fila.get('messages', [])

        nova_lista = [
            msg for msg in mensagens
            if msg.get('ReceiptHandle') != receipt_handle
        ]

        deletadas = len(mensagens) - len(nova_lista)
        dados_fila['messages'] = nova_lista

        if deletadas > 0:
            dados_fila['stats']['total_deleted'] = \
                dados_fila['stats'].get('total_deleted', 0) + deletadas
            self._salvar_fila(queue_name, dados_fila)
            logger.debug(f"Mensagem deletada de '{queue_name}'")
            return True

        logger.warning(f"ReceiptHandle não encontrado em '{queue_name}'")
        return False

    def _mover_para_dlq(self, queue_name: str, msg: Dict, dados_fila: Dict):
        """Move uma mensagem para a Dead Letter Queue."""
        msg_dlq = msg.copy()
        msg_dlq['dlq_timestamp'] = datetime.now().isoformat()
        msg_dlq['source_queue'] = queue_name

        dados_fila.setdefault('dead_letters', []).append(msg_dlq)
        dados_fila['messages'] = [
            m for m in dados_fila['messages']
            if m['MessageId'] != msg['MessageId']
        ]
        dados_fila['stats']['total_dlq'] = \
            dados_fila['stats'].get('total_dlq', 0) + 1

        # Também salva na fila DLQ separada
        self.send_message('dlq_errors', json.loads(msg['Body']) if msg['Body'] else {})
        logger.warning(f"Mensagem movida para DLQ: {msg['MessageId'][:8]}")

    def get_queue_attributes(self, queue_name: str) -> Dict:
        """
        Retorna atributos e métricas de uma fila.

        AWS boto3 equivalente:
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )

        Útil para monitoramento: quantas mensagens na fila, etc.
        """
        dados_fila = self._ler_fila(queue_name)
        mensagens = dados_fila.get('messages', [])
        agora = datetime.now()

        # Calcula mensagens visíveis e invisíveis
        visiveis = 0
        invisiveis = 0
        for msg in mensagens:
            deadline = msg.get('VisibilityDeadline')
            if deadline and datetime.fromisoformat(deadline) > agora:
                invisiveis += 1
            else:
                visiveis += 1

        stats = dados_fila.get('stats', {})

        return {
            'QueueName': queue_name,
            'ApproximateNumberOfMessages': str(visiveis),
            'ApproximateNumberOfMessagesNotVisible': str(invisiveis),
            'ApproximateNumberOfMessagesDelayed': '0',
            'CreatedTimestamp': dados_fila.get('created_at', ''),
            'TotalMessagesSent': str(stats.get('total_sent', 0)),
            'TotalMessagesReceived': str(stats.get('total_received', 0)),
            'TotalMessagesDeleted': str(stats.get('total_deleted', 0)),
            'DeadLetterMessages': str(len(dados_fila.get('dead_letters', []))),
        }

    def purge_queue(self, queue_name: str) -> bool:
        """
        Deleta todas as mensagens de uma fila.

        No SQS real: operação pode demorar até 60 segundos.
        Útil para testes e desenvolvimento.
        """
        dados_fila = self._ler_fila(queue_name)
        qtd = len(dados_fila.get('messages', []))
        dados_fila['messages'] = []
        self._salvar_fila(queue_name, dados_fila)
        logger.warning(f"Fila '{queue_name}' limpa! {qtd} mensagens removidas.")
        return True


# =============================================================================
# TESTE E DEMONSTRAÇÃO
# =============================================================================
if __name__ == '__main__':
    print("=== Testando SQSLocal ===\n")

    sqs = SQSLocal()

    # 1. Enviar mensagens
    print("1. Enviando 3 mensagens de venda...")
    for i in range(1, 4):
        sqs.send_message('vendas_queue', {
            'id_venda': f'VND-2024-{i:06d}',
            'cpf_cliente': f'1234567890{i}',
            'valor_total': 100.00 * i,
            'data_venda': datetime.now().isoformat(),
            'canal_venda': 'ECOMMERCE'
        })

    # 2. Ver atributos
    print("\n2. Atributos da fila:")
    attrs = sqs.get_queue_attributes('vendas_queue')
    for k, v in attrs.items():
        if not k.startswith('_'):
            print(f"   {k}: {v}")

    # 3. Receber mensagens
    print("\n3. Recebendo mensagens...")
    mensagens = sqs.receive_messages('vendas_queue', max_messages=2)
    for msg in mensagens:
        body = json.loads(msg['Body'])
        print(f"   ID: {body['id_venda']}, Valor: R${body['valor_total']}")

        # 4. Confirmar processamento
        sqs.delete_message('vendas_queue', msg['ReceiptHandle'])
        print(f"   Processada e deletada!")

    # 5. Ver atributos após processamento
    print("\n4. Atributos após processamento:")
    attrs = sqs.get_queue_attributes('vendas_queue')
    print(f"   Mensagens restantes: {attrs['ApproximateNumberOfMessages']}")

    print("\n=== Teste concluído! ===")
