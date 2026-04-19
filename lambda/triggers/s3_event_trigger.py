"""
s3_event_trigger.py - Simula o trigger de eventos S3 → Lambda

CONCEITO AWS:
    No AWS real, você configura o S3 para notificar o Lambda:
        S3 Console → Bucket → Properties → Event notifications
        → Add notification:
           Events: s3:ObjectCreated:*
           Prefix: raw/
           Suffix: .json
           Destination: Lambda function → validate_schema

    O Lambda recebe automaticamente um evento JSON com:
    {
        "Records": [{
            "s3": {
                "bucket": {"name": "meu-bucket"},
                "object": {"key": "raw/clientes/arquivo.json", "size": 1234}
            }
        }]
    }

SIMULAÇÃO LOCAL:
    Este script monitora a pasta s3/raw/ por novos arquivos.
    Quando detecta um arquivo novo, simula o evento S3 e chama
    as funções Lambda registradas.

    É equivalente ao que a AWS faz internamente quando você
    configura S3 Event Notifications.
"""

import os
import sys
import time
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Callable, Dict, List, Set

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [S3-TRIGGER] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('S3EventTrigger')


def criar_evento_s3(bucket_name: str, key: str,
                    event_type: str = 'ObjectCreated:Put',
                    file_size: int = 0) -> Dict:
    """
    Cria um evento S3 no mesmo formato que a AWS enviaria para o Lambda.

    No AWS, este JSON é gerado automaticamente pelo S3 e enviado
    para o Lambda via SQS interno da AWS (transparente para o usuário).

    Args:
        bucket_name: Nome do bucket S3
        key: Chave do objeto (caminho do arquivo)
        event_type: Tipo do evento (ObjectCreated:Put, ObjectRemoved:Delete, etc.)
        file_size: Tamanho do arquivo em bytes

    Returns:
        Dict no formato de evento S3 da AWS
    """
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": datetime.now().isoformat() + "Z",
                "eventName": event_type,
                "userIdentity": {
                    "principalId": "EXEMPLO_USER_ID"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": hashlib.md5(key.encode()).hexdigest(),
                    "x-amz-id-2": "simulacao_local"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "S3EventNotification",
                    "bucket": {
                        "name": bucket_name,
                        "ownerIdentity": {"principalId": "EXEMPLO_OWNER"},
                        "arn": f"arn:aws:s3:::{bucket_name}"
                    },
                    "object": {
                        "key": key,
                        "size": file_size,
                        "eTag": hashlib.md5(key.encode()).hexdigest(),
                        "sequencer": datetime.now().strftime('%Y%m%d%H%M%S')
                    }
                }
            }
        ]
    }


class S3EventTrigger:
    """
    Monitor de pasta que simula o S3 Event Notifications da AWS.

    Funciona como um "observador" de arquivos (file watcher):
    - Mantém um registro dos arquivos já processados
    - Detecta novos arquivos
    - Dispara as funções Lambda registradas

    No AWS real, isso é feito internamente pelo S3.
    """

    def __init__(self,
                 bucket_name: str = 'data-platform-local',
                 watch_path: str = None,
                 prefixo_monitorado: str = 'raw/'):
        """
        Inicializa o monitor de eventos S3.

        Args:
            bucket_name: Nome fictício do bucket (para o evento)
            watch_path: Pasta a monitorar (padrão: s3/raw/)
            prefixo_monitorado: Prefixo do S3 a monitorar
        """
        if watch_path is None:
            watch_path = str(projeto_root / 's3' / 'raw')

        self.bucket_name = bucket_name
        self.watch_path = Path(watch_path)
        self.prefixo = prefixo_monitorado

        # Funções Lambda registradas para serem chamadas nos eventos
        # No AWS: você registra via console ou terraform
        self.handlers: List[Callable] = []

        # Registro de arquivos já processados (para evitar reprocessamento)
        self.processed_files: Set[str] = set()
        self._carregar_estado()

        logger.info(f"Monitor S3 iniciado. Monitorando: {self.watch_path}")

    def registrar_handler(self, func: Callable,
                          suffixos: List[str] = None,
                          prefixos: List[str] = None):
        """
        Registra uma função Lambda para ser chamada quando um evento ocorrer.

        No AWS real, você configura isso via:
            s3.put_bucket_notification_configuration(
                Bucket='meu-bucket',
                NotificationConfiguration={
                    'LambdaFunctionConfigurations': [{
                        'LambdaFunctionArn': 'arn:aws:lambda:...',
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {'Name': 'prefix', 'Value': 'raw/'},
                                    {'Name': 'suffix', 'Value': '.json'}
                                ]
                            }
                        }
                    }]
                }
            )

        Args:
            func: Função Python que simula o Lambda
            suffixos: Lista de sufixos para filtrar (ex: ['.json', '.csv'])
            prefixos: Lista de prefixos para filtrar (ex: ['raw/clientes/'])
        """
        self.handlers.append({
            'func': func,
            'suffixos': suffixos or [],
            'prefixos': prefixos or []
        })
        logger.info(f"Handler registrado: {func.__name__}")

    def _deve_processar(self, key: str, handler_config: Dict) -> bool:
        """Verifica se um arquivo deve ser processado por um handler específico."""
        suffixos = handler_config.get('suffixos', [])
        prefixos = handler_config.get('prefixos', [])

        # Se não tem filtros, processa tudo
        if not suffixos and not prefixos:
            return True

        # Verifica sufixo
        if suffixos and not any(key.endswith(s) for s in suffixos):
            return False

        # Verifica prefixo
        if prefixos and not any(key.startswith(p) for p in prefixos):
            return False

        return True

    def verificar_novos_arquivos(self) -> List[Dict]:
        """
        Verifica se há novos arquivos na pasta monitorada.

        Returns:
            Lista de eventos S3 para novos arquivos
        """
        novos_eventos = []

        if not self.watch_path.exists():
            return novos_eventos

        for arquivo in self.watch_path.rglob('*'):
            if arquivo.is_dir() or arquivo.suffix == '.meta':
                continue

            # Converte caminho local para key S3
            caminho_relativo = str(arquivo.relative_to(self.watch_path.parent))
            key = caminho_relativo.replace(os.sep, '/')

            # Pula arquivos já processados
            if key in self.processed_files:
                continue

            # Cria evento S3 para o novo arquivo
            evento = criar_evento_s3(
                bucket_name=self.bucket_name,
                key=key,
                event_type='ObjectCreated:Put',
                file_size=arquivo.stat().st_size
            )
            novos_eventos.append(evento)

        return novos_eventos

    def processar_evento(self, evento: Dict):
        """
        Processa um evento S3 chamando os handlers registrados.

        No AWS: o Lambda recebe o evento via invocação assíncrona
        e tem 2 tentativas de retry em caso de falha.
        """
        for record in evento.get('Records', []):
            key = record['s3']['object']['key']
            event_name = record['eventName']
            file_size = record['s3']['object']['size']

            logger.info(f"Evento S3: {event_name} → {key} ({file_size} bytes)")

            # Chama cada handler registrado
            for handler_config in self.handlers:
                if not self._deve_processar(key, handler_config):
                    continue

                func = handler_config['func']
                logger.info(f"  Invocando Lambda: {func.__name__}")

                try:
                    # Simula a invocação assíncrona do Lambda
                    resultado = func(evento)
                    logger.info(f"  {func.__name__} executado: {resultado}")
                except Exception as e:
                    # No AWS: Lambda tenta novamente 2 vezes automaticamente
                    # Após 2 falhas: mensagem vai para DLQ (Dead Letter Queue)
                    logger.error(f"  ERRO em {func.__name__}: {e}")
                    logger.error(f"  (No AWS: tentativa 1/2, depois DLQ)")

            # Marca como processado
            self.processed_files.add(key)

        self._salvar_estado()

    def monitorar_continuo(self, intervalo_segundos: int = 5,
                           max_iteracoes: int = None):
        """
        Monitora a pasta continuamente em loop.

        No AWS: o S3 detecta eventos instantaneamente via infra interna.
        Aqui polling a cada N segundos (boa aproximação para fins educacionais).

        Args:
            intervalo_segundos: Frequência de verificação
            max_iteracoes: Para teste, limita o número de loops
        """
        logger.info(f"Iniciando monitoramento contínuo (a cada {intervalo_segundos}s)")
        logger.info("Pressione Ctrl+C para parar")

        iteracao = 0
        try:
            while True:
                novos_eventos = self.verificar_novos_arquivos()

                if novos_eventos:
                    logger.info(f"Detectados {len(novos_eventos)} novos arquivos!")
                    for evento in novos_eventos:
                        self.processar_evento(evento)
                else:
                    logger.debug("Nenhum arquivo novo detectado")

                iteracao += 1
                if max_iteracoes and iteracao >= max_iteracoes:
                    break

                time.sleep(intervalo_segundos)

        except KeyboardInterrupt:
            logger.info("Monitoramento encerrado pelo usuário")

    def _carregar_estado(self):
        """Carrega registro de arquivos já processados."""
        estado_path = projeto_root / 'lambda' / 'triggers' / '.s3_trigger_state.json'
        if estado_path.exists():
            with open(estado_path, 'r') as f:
                state = json.load(f)
                self.processed_files = set(state.get('processed_files', []))

    def _salvar_estado(self):
        """Salva registro de arquivos processados (persiste entre execuções)."""
        estado_path = projeto_root / 'lambda' / 'triggers' / '.s3_trigger_state.json'
        estado_path.parent.mkdir(parents=True, exist_ok=True)
        with open(estado_path, 'w') as f:
            json.dump(
                {'processed_files': list(self.processed_files),
                 'updated_at': datetime.now().isoformat()},
                f, indent=2
            )


# =============================================================================
# DEMONSTRAÇÃO DE USO
# =============================================================================
if __name__ == '__main__':
    # Importa as funções Lambda que queremos chamar
    sys.path.insert(0, str(projeto_root / 'lambda' / 'functions'))
    from validate_schema import lambda_handler as validar_schema

    # Cria o trigger
    trigger = S3EventTrigger()

    # Registra handlers (funções Lambda)
    trigger.registrar_handler(
        validar_schema,
        suffixos=['.json', '.csv'],
        prefixos=['raw/']
    )

    # Executa uma verificação única (para teste)
    print("\n=== Verificando novos arquivos em s3/raw/ ===")
    novos = trigger.verificar_novos_arquivos()
    print(f"Novos arquivos detectados: {len(novos)}")

    for evento in novos:
        trigger.processar_evento(evento)

    print("\nPara monitoramento contínuo, use:")
    print("  trigger.monitorar_continuo(intervalo_segundos=5)")
