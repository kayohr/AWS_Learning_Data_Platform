# Streaming - Simulação de Jitsu/Kafka (Amazon Kinesis)

## Conceito: Streaming vs Batch

```
BATCH (o que os outros módulos fazem):
    Dados acumulam → a cada hora/dia → processa tudo de uma vez
    Latência: minutos a horas
    Ex: ETL noturno, relatórios diários

STREAMING (este módulo):
    Dados chegam → processa IMEDIATAMENTE, um a um
    Latência: milissegundos a segundos
    Ex: fraude em transações, feed de notícias, jogos online
```

## Kafka Local = Amazon Kinesis

| Conceito Kafka    | Conceito Kinesis    | Descrição                              |
|-------------------|---------------------|----------------------------------------|
| Topic             | Stream              | Canal de dados nomeado                 |
| Partition         | Shard               | Subdivide o stream para paralelismo    |
| Producer          | Producer            | Quem envia dados                       |
| Consumer          | Consumer            | Quem lê dados                          |
| Consumer Group    | -                   | Grupo que divide partições entre si    |
| Offset            | Sequence Number     | Posição na fila                        |
| Broker            | -                   | Servidor Kafka (Kinesis não tem)       |
| Zookeeper         | -                   | Coordenação (Kinesis não precisa)      |

## Tópicos Kafka (Streams Kinesis) neste projeto

```
eventos-website     → Cliques, page views, buscas
eventos-vendas      → Compras concluídas em tempo real
```

## Produtores (como dados entram)

No AWS:
```python
import boto3
kinesis = boto3.client('kinesis')
kinesis.put_record(
    StreamName='eventos-website',
    Data=json.dumps(evento),
    PartitionKey=str(evento['user_id'])  # Garante que eventos do mesmo user vão para o mesmo shard
)
```

Localmente (Kafka):
```python
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('eventos-website', json.dumps(evento).encode())
```

## Consumidores (como dados são processados)

No AWS:
```python
# Kinesis com Lambda (serverless, automático!)
# Lambda é disparada para cada batch de records
def lambda_handler(event, context):
    for record in event['Records']:
        dados = base64.b64decode(record['kinesis']['data'])
        evento = json.loads(dados)
        processar_evento(evento)
```

Localmente (Kafka):
```python
from kafka import KafkaConsumer
consumer = KafkaConsumer('eventos-website', group_id='analytics')
for msg in consumer:
    evento = json.loads(msg.value)
    processar_evento(evento)
```

## Jitsu - Coleta de Eventos Web

**Jitsu** é uma ferramenta open-source para coleta de eventos de usuário (como o Segment).

```javascript
// No site, adiciona 1 linha de JavaScript:
<script src="https://t.jitsu.com/s/lib.js" data-key="SEU_KEY" defer></script>

// Jitsu captura automaticamente:
// - Page views
// - Cliques
// - Tempo na página
// E envia para o servidor Jitsu → Kafka → S3/Redshift
```

No AWS: isso seria feito com:
- **Amazon Pinpoint**: analytics de eventos de usuário
- **AWS Amplify**: SDK para apps web/mobile
- **API Gateway + Lambda**: endpoint customizado → Kinesis

## Arquitetura de Streaming
```
Website/App                       Kafka                     Destinos
──────────   ────────────────→  ──────────  ──────────→  ──────────────
User click   evento_click        eventos-   consumer     s3/raw/eventos/
User buy     producer.py        website    _eventos.py   PostgreSQL
             evento_compra                  consumer    Aggregation
             producer.py        eventos-   _analytics   cache
                                vendas     .py
```
