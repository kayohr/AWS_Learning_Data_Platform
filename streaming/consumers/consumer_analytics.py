"""
consumer_analytics.py - Consumer com agregações em tempo real

CONCEITO STREAM PROCESSING:
    No AWS: Kinesis Data Analytics (Apache Flink gerenciado)
    Localmente: Pandas com janelas de tempo

    Agregações em tempo real:
        - Contagem de eventos por minuto (throughput)
        - Receita acumulada do dia
        - Top 5 produtos mais vistos
        - Alertas: queda brusca de eventos
"""

import sys
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict

projeto_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(projeto_root))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [ANALYTICS] %(levelname)s: %(message)s')
logger = logging.getLogger('ConsumerAnalytics')


class StreamAnalytics:
    """
    Processa eventos em streaming e mantém métricas em tempo real.

    No AWS Kinesis Analytics:
        CREATE STREAM "processed_events" AS
        SELECT STREAM
            event_type,
            COUNT(*) OVER (RANGE INTERVAL '1' MINUTE PRECEDING) AS events_per_minute
        FROM "SOURCE_SQL_STREAM_001";
    """

    def __init__(self):
        self.eventos_por_tipo = Counter()
        self.receita_acumulada = 0.0
        self.produtos_vistos = Counter()
        self.eventos_por_minuto = defaultdict(int)
        self.ultimo_reset = datetime.now()

    def processar_evento(self, evento: Dict):
        tipo = evento.get('event_type', 'DESCONHECIDO')
        self.eventos_por_tipo[tipo] += 1

        minuto_atual = datetime.now().strftime('%H:%M')
        self.eventos_por_minuto[minuto_atual] += 1

        if tipo == 'PURCHASE_COMPLETED':
            self.receita_acumulada += evento.get('valor_total', 0)

        produto_id = evento.get('properties', {}).get('produto_id') or \
                     evento.get('produto', {}).get('id')
        if produto_id:
            self.produtos_vistos[produto_id] += 1

    def get_dashboard(self) -> Dict:
        """Retorna painel de métricas atuais."""
        total_eventos = sum(self.eventos_por_tipo.values())
        top_produtos = self.produtos_vistos.most_common(5)

        return {
            'timestamp': datetime.now().isoformat(),
            'total_eventos': total_eventos,
            'receita_acumulada': round(self.receita_acumulada, 2),
            'eventos_por_tipo': dict(self.eventos_por_tipo),
            'top_5_produtos': dict(top_produtos),
            'eventos_ultimo_minuto': self.eventos_por_minuto.get(
                datetime.now().strftime('%H:%M'), 0
            )
        }

    def imprimir_dashboard(self):
        """Imprime dashboard no terminal."""
        d = self.get_dashboard()
        print("\n" + "="*50)
        print(f"ANALYTICS EM TEMPO REAL - {d['timestamp'][:19]}")
        print("="*50)
        print(f"Total de eventos    : {d['total_eventos']:,}")
        print(f"Receita acumulada   : R$ {d['receita_acumulada']:,.2f}")
        print(f"Eventos/min atual   : {d['eventos_ultimo_minuto']}")
        print("\nTop produtos:")
        for prod, qtd in d['top_5_produtos'].items():
            print(f"  {prod}: {qtd} visualizações")
        print("="*50)


def processar_arquivos_locais():
    """Processa eventos salvos localmente para analytics."""
    analytics = StreamAnalytics()

    eventos_dir = projeto_root / 's3' / 'raw' / 'eventos'
    if not eventos_dir.exists():
        logger.warning("Sem eventos para processar. Execute: python streaming/producers/evento_click_producer.py")
        return

    arquivos = list(eventos_dir.glob('*.jsonl'))
    total = 0

    for arquivo in sorted(arquivos):
        with open(arquivo, 'r', encoding='utf-8') as f:
            for linha in f:
                if linha.strip():
                    evento = json.loads(linha)
                    analytics.processar_evento(evento)
                    total += 1

    logger.info(f"Processados {total} eventos dos arquivos locais")
    analytics.imprimir_dashboard()

    # Salva resultado
    output_dir = projeto_root / 'monitoring' / 'analytics'
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    with open(output_dir / f'analytics_{ts}.json', 'w') as f:
        json.dump(analytics.get_dashboard(), f, ensure_ascii=False, indent=2)

    return analytics.get_dashboard()


if __name__ == '__main__':
    processar_arquivos_locais()
