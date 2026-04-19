"""
consumer_eventos.py - Consome eventos do Kafka e salva em s3/raw/eventos/

CONCEITO KINESIS CONSUMER:
    No AWS, o Lambda seria disparado automaticamente pelo Kinesis:
        Kinesis Stream → Lambda Trigger → Processa batch de records
        Custo: Lambda processa grátis até 1M invocações/mês

    Localmente: script Python que lê do Kafka continuamente.

    O Consumer Group garante que cada mensagem seja processada
    apenas uma vez, mesmo com múltiplos consumers rodando.
"""

import sys
import json
import logging
from datetime import datetime
from pathlib import Path

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [CONSUMER-EVENTOS] %(levelname)s: %(message)s')
logger = logging.getLogger('ConsumerEventos')


def processar_batch_eventos(eventos: list) -> int:
    """
    Processa um batch de eventos e salva no S3 local.
    No Kinesis Lambda: seria chamada para cada batch automaticamente.
    """
    from s3.scripts.s3_utils import S3Local

    s3 = S3Local()
    hora = datetime.now().strftime('%Y%m%d_%H')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Agrupa por tipo de evento
    por_tipo = {}
    for evento in eventos:
        tipo = evento.get('event_type', 'desconhecido')
        por_tipo.setdefault(tipo, []).append(evento)

    arquivos_criados = 0
    for tipo, lista_eventos in por_tipo.items():
        key = f"raw/eventos/{tipo.lower()}/{hora}/{tipo}_{timestamp}.jsonl"
        conteudo = '\n'.join(json.dumps(e, ensure_ascii=False) for e in lista_eventos)
        s3.put_object(key, conteudo.encode('utf-8'))
        arquivos_criados += 1

    return arquivos_criados


def consumir_kafka(topic: str = 'eventos-website',
                   group_id: str = 'consumer-s3-writer',
                   max_mensagens: int = None):
    """Consome mensagens do Kafka e salva no S3."""
    try:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=100
        )

        logger.info(f"Consumindo tópico '{topic}' (grupo: {group_id})")

        count = 0
        buffer = []

        for mensagem in consumer:
            buffer.append(mensagem.value)
            count += 1

            # Processa em batch de 100
            if len(buffer) >= 100:
                processar_batch_eventos(buffer)
                logger.info(f"Batch processado: {len(buffer)} eventos")
                buffer = []

            if max_mensagens and count >= max_mensagens:
                break

        # Processa restantes
        if buffer:
            processar_batch_eventos(buffer)

        consumer.close()
        logger.info(f"Total consumido: {count} eventos")

    except Exception as e:
        logger.warning(f"Kafka indisponível: {e}")
        logger.info("Para testar sem Kafka: use os arquivos em s3/raw/eventos/")


def consumir_arquivos_locais():
    """
    Fallback: processa arquivos JSONL já salvos em s3/raw/eventos/.
    Simula o que aconteceria se o consumer processasse o backlog.
    """
    eventos_dir = projeto_root / 's3' / 'raw' / 'eventos'

    if not eventos_dir.exists():
        logger.warning("Pasta s3/raw/eventos/ não existe. Execute o produtor primeiro.")
        return

    arquivos = list(eventos_dir.glob('*.jsonl'))
    if not arquivos:
        logger.info("Nenhum arquivo .jsonl em s3/raw/eventos/")
        return

    total_eventos = 0
    for arquivo in arquivos:
        with open(arquivo, 'r', encoding='utf-8') as f:
            eventos = [json.loads(linha) for linha in f if linha.strip()]
        total_eventos += len(eventos)
        logger.info(f"Processado: {arquivo.name} ({len(eventos)} eventos)")

    logger.info(f"Total processado dos arquivos locais: {total_eventos} eventos")
    return total_eventos


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Consumer de eventos')
    parser.add_argument('--modo', choices=['kafka', 'arquivo'], default='arquivo')
    parser.add_argument('--mensagens', type=int, default=None)
    args = parser.parse_args()

    if args.modo == 'kafka':
        consumir_kafka(max_mensagens=args.mensagens)
    else:
        consumir_arquivos_locais()
