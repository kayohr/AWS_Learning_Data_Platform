"""
evento_compra_producer.py - Produtor de eventos de compra em tempo real

Simula compras sendo registradas instantaneamente no Kafka.
No AWS: seria o Kinesis Data Streams recebendo eventos do checkout.
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
from typing import Dict

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [PRODUCER-COMPRA] %(levelname)s: %(message)s')
logger = logging.getLogger('EventoCompraProducer')

PRODUTOS = [
    {'id': 'PROD-001', 'nome': 'Smartphone Samsung Galaxy', 'categoria': 'Eletrônicos',
     'preco_min': 800, 'preco_max': 3500},
    {'id': 'PROD-002', 'nome': 'Notebook Dell Inspiron', 'categoria': 'Eletrônicos',
     'preco_min': 2500, 'preco_max': 6000},
    {'id': 'PROD-003', 'nome': 'Tênis Nike Air Max', 'categoria': 'Calçados',
     'preco_min': 250, 'preco_max': 800},
    {'id': 'PROD-004', 'nome': 'Camiseta Polo Ralph Lauren', 'categoria': 'Vestuário',
     'preco_min': 150, 'preco_max': 400},
    {'id': 'PROD-005', 'nome': 'Fone Bluetooth JBL', 'categoria': 'Eletrônicos',
     'preco_min': 200, 'preco_max': 600},
    {'id': 'PROD-006', 'nome': 'Cafeteira Nespresso', 'categoria': 'Casa',
     'preco_min': 350, 'preco_max': 900},
]

FORMAS_PAGAMENTO = ['CARTAO_CREDITO', 'PIX', 'BOLETO', 'CARTAO_DEBITO']
CANAIS = ['ECOMMERCE', 'APP_MOBILE', 'TELEVENDAS']


def gerar_cpf_fake() -> str:
    """Gera um CPF fictício (apenas para simulação)."""
    nums = [random.randint(0, 9) for _ in range(11)]
    return ''.join(map(str, nums))


def gerar_evento_compra() -> Dict:
    """Gera um evento de compra realista."""
    produto = random.choice(PRODUTOS)
    qtd = random.randint(1, 3)
    valor_unit = round(random.uniform(produto['preco_min'], produto['preco_max']), 2)
    desconto = round(valor_unit * random.uniform(0, 0.15), 2) if random.random() > 0.7 else 0
    valor_total = round((valor_unit * qtd) - desconto, 2)

    return {
        "_id": str(uuid.uuid4()),
        "event_type": "PURCHASE_COMPLETED",
        "timestamp": datetime.now().isoformat(),
        "id_venda": f"VND-{datetime.now().strftime('%Y%m%d')}-{random.randint(10000, 99999)}",
        "cpf_cliente": gerar_cpf_fake(),
        "produto": {
            "id": produto['id'],
            "nome": produto['nome'],
            "categoria": produto['categoria'],
        },
        "quantidade": qtd,
        "valor_unitario": valor_unit,
        "desconto": desconto,
        "valor_total": valor_total,
        "forma_pagamento": random.choice(FORMAS_PAGAMENTO),
        "parcelas": random.choice([1, 1, 1, 2, 3, 6, 12]) if
                    random.choice(FORMAS_PAGAMENTO) == 'CARTAO_CREDITO' else 1,
        "canal": random.choice(CANAIS),
        "cidade": random.choice(['São Paulo', 'Rio de Janeiro', 'Belo Horizonte',
                                  'Curitiba', 'Porto Alegre'])
    }


def salvar_em_arquivo(evento: Dict):
    """Salva evento de compra em arquivo (fallback quando Kafka offline)."""
    pasta = projeto_root / 's3' / 'raw' / 'eventos'
    pasta.mkdir(parents=True, exist_ok=True)
    hora = datetime.now().strftime('%Y%m%d_%H')
    arquivo = pasta / f"compras_{hora}.jsonl"
    with open(arquivo, 'a', encoding='utf-8') as f:
        f.write(json.dumps(evento, ensure_ascii=False) + '\n')


def produzir_compras(qtd: int = 20, intervalo_ms: int = 200):
    """Produz eventos de compra."""
    logger.info(f"Produzindo {qtd} eventos de compra...")

    total_receita = 0
    for i in range(qtd):
        evento = gerar_evento_compra()
        salvar_em_arquivo(evento)
        total_receita += evento['valor_total']
        logger.info(
            f"Compra {i+1}/{qtd}: {evento['produto']['nome'][:30]} "
            f"R${evento['valor_total']:.2f}"
        )
        time.sleep(intervalo_ms / 1000)

    logger.info(f"Concluído! Total: {qtd} compras, Receita: R${total_receita:,.2f}")
    return qtd


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Produtor de eventos de compra')
    parser.add_argument('--eventos', type=int, default=20)
    parser.add_argument('--intervalo', type=int, default=200)
    args = parser.parse_args()

    produzir_compras(qtd=args.eventos, intervalo_ms=args.intervalo)
