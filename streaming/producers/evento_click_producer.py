"""
evento_click_producer.py - Simula eventos de clique no website → Kafka

CONCEITO AWS KINESIS:
    No AWS, eventos de clique seriam enviados para o Kinesis Data Streams:
        kinesis_client.put_records(
            StreamName='eventos-website',
            Records=[
                {
                    'Data': json.dumps(evento).encode(),
                    'PartitionKey': str(evento['user_id'])
                }
            ]
        )

    O Kinesis tem um conceito de 'PartitionKey':
    - Todos os eventos com o mesmo PartitionKey vão para o mesmo shard
    - Isso garante ordenação por usuário (todos os cliques do user=123 ficam ordenados)

    Localmente: usamos Kafka com a mesma lógica de chave de partição.

USO:
    python streaming/producers/evento_click_producer.py              # Produz infinitamente
    python streaming/producers/evento_click_producer.py --eventos 100  # Produz 100 eventos
    python streaming/producers/evento_click_producer.py --modo arquivo  # Salva em S3 local
"""

import sys
import json
import time
import random
import uuid
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PRODUCER-CLICK] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('EventoClickProducer')

# Dados para simulação realista de eventos brasileiros
PAGINAS = [
    '/', '/produtos', '/produtos/eletronicos', '/produtos/roupas',
    '/carrinho', '/checkout', '/minha-conta', '/pedidos',
    '/busca', '/ofertas', '/marcas'
]

DISPOSITIVOS = ['desktop', 'mobile', 'tablet']
NAVEGADORES = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Samsung Internet']
SISTEMAS_OPERACIONAIS = ['Windows 11', 'macOS', 'Android 13', 'iOS 17', 'Ubuntu']
CIDADES_BRASILEIRAS = [
    'São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Curitiba',
    'Porto Alegre', 'Salvador', 'Recife', 'Fortaleza', 'Manaus', 'Belém'
]
UTM_SOURCES = ['google', 'facebook', 'instagram', 'email', 'direto', 'orgânico']
TIPOS_CLICK = [
    'PAGE_VIEW', 'BUTTON_CLICK', 'PRODUCT_VIEW', 'ADD_TO_CART',
    'REMOVE_FROM_CART', 'SEARCH', 'MENU_CLICK', 'BANNER_CLICK'
]


def gerar_evento_click(user_id: str = None) -> Dict:
    """
    Gera um evento de clique realista simulando comportamento de usuário.

    A estrutura do evento segue o padrão do Jitsu/Segment:
    {
        "_id": UUID único do evento,
        "event_type": tipo do evento,
        "timestamp": quando aconteceu,
        "user_id": quem fez,
        "session_id": sessão do navegador,
        "properties": dados específicos do evento
    }
    """
    if user_id is None:
        user_id = f"usr_{random.randint(1000, 99999)}"

    agora = datetime.now()

    evento = {
        "_id": str(uuid.uuid4()),
        "event_type": random.choice(TIPOS_CLICK),
        "timestamp": agora.isoformat(),
        "user_id": user_id,
        "session_id": f"sess_{random.randint(100000, 999999)}",

        "page": {
            "url": f"https://loja.exemplo.com.br{random.choice(PAGINAS)}",
            "path": random.choice(PAGINAS),
            "title": "Loja Exemplo - Melhores Preços do Brasil",
            "referrer": random.choice([
                "https://www.google.com",
                "https://www.instagram.com",
                "",  # acesso direto
            ])
        },

        "device": {
            "type": random.choice(DISPOSITIVOS),
            "browser": random.choice(NAVEGADORES),
            "os": random.choice(SISTEMAS_OPERACIONAIS),
            "screen_width": random.choice([1920, 1440, 1366, 414, 375]),
            "screen_height": random.choice([1080, 900, 768, 896, 812])
        },

        "location": {
            "country": "Brasil",
            "city": random.choice(CIDADES_BRASILEIRAS),
            "timezone": "America/Sao_Paulo"
        },

        "utm": {
            "source": random.choice(UTM_SOURCES),
            "medium": random.choice(['cpc', 'social', 'email', 'organic', None]),
            "campaign": random.choice(['black_friday', 'natal', 'dia_dos_pais', None])
        },

        "properties": {
            "tempo_pagina_ms": random.randint(1000, 120000),
            "scroll_depth_pct": random.randint(10, 100),
            "produto_id": f"PROD-{random.randint(100, 999)}" if random.random() > 0.5 else None,
        }
    }

    return evento


def publicar_kafka(evento: Dict, topic: str = 'eventos-website') -> bool:
    """
    Publica evento no Kafka (equivale ao Kinesis put_record no AWS).

    A 'partition_key' (user_id) garante que todos os eventos do mesmo usuário
    vão para a mesma partição (preservando a ordem).
    """
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable

        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',          # Aguarda confirmação de todos os replicas
            retries=3,
            compression_type='gzip'
        )

        future = producer.send(
            topic,
            value=evento,
            key=evento.get('user_id')  # Partition key = user_id
        )

        # Aguarda confirmação (bloqueia por até 5 segundos)
        record_metadata = future.get(timeout=5)

        logger.debug(
            f"Evento enviado: topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}"
        )
        producer.close()
        return True

    except Exception as e:
        logger.warning(f"Kafka indisponível ({e}), salvando localmente...")
        return False


def salvar_em_arquivo(evento: Dict, pasta: str = None):
    """
    Fallback: salva evento em arquivo JSON quando Kafka não está disponível.
    Simula o que o Kinesis Firehose faria: bufferizar e salvar no S3.
    """
    if pasta is None:
        pasta = str(projeto_root / 's3' / 'raw' / 'eventos')

    Path(pasta).mkdir(parents=True, exist_ok=True)

    # Agrupa por hora para evitar muitos arquivos pequenos
    hora = datetime.now().strftime('%Y%m%d_%H')
    arquivo = Path(pasta) / f"eventos_{hora}.jsonl"  # JSONL = JSON Lines

    with open(arquivo, 'a', encoding='utf-8') as f:
        f.write(json.dumps(evento, ensure_ascii=False) + '\n')


def produzir_eventos(qtd_eventos: int = None, intervalo_ms: int = 100,
                      modo: str = 'kafka'):
    """
    Produz eventos continuamente ou em quantidade definida.

    Args:
        qtd_eventos: Número de eventos a produzir (None = infinito)
        intervalo_ms: Intervalo em milissegundos entre eventos
        modo: 'kafka' (tenta Kafka, fallback arquivo) ou 'arquivo' (sempre arquivo)
    """
    count = 0
    erros_kafka = 0

    logger.info(f"Iniciando produtor de eventos de clique")
    logger.info(f"Modo: {modo}, Intervalo: {intervalo_ms}ms")
    if qtd_eventos:
        logger.info(f"Produzindo {qtd_eventos} eventos")
    else:
        logger.info("Produção contínua (Ctrl+C para parar)")

    try:
        while qtd_eventos is None or count < qtd_eventos:
            # Gera evento
            user_id = f"usr_{random.randint(1000, 9999)}"
            evento = gerar_evento_click(user_id)

            # Publica
            if modo == 'kafka':
                sucesso = publicar_kafka(evento)
                if not sucesso:
                    erros_kafka += 1
                    salvar_em_arquivo(evento)  # Fallback para arquivo
            else:
                salvar_em_arquivo(evento)

            count += 1

            if count % 100 == 0:
                logger.info(f"Eventos produzidos: {count} (erros Kafka: {erros_kafka})")

            time.sleep(intervalo_ms / 1000)

    except KeyboardInterrupt:
        logger.info(f"Produção encerrada. Total: {count} eventos")

    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Produtor de eventos de clique (simula website → Kafka/Kinesis)'
    )
    parser.add_argument('--eventos', type=int, default=50,
                       help='Número de eventos (padrão: 50, 0=infinito)')
    parser.add_argument('--intervalo', type=int, default=50,
                       help='Intervalo entre eventos em ms (padrão: 50ms)')
    parser.add_argument('--modo', choices=['kafka', 'arquivo'], default='arquivo',
                       help='Modo de publicação (padrão: arquivo)')

    args = parser.parse_args()
    qtd = args.eventos if args.eventos > 0 else None

    produzir_eventos(qtd_eventos=qtd, intervalo_ms=args.intervalo, modo=args.modo)
