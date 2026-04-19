# SQS - Simulação do Amazon SQS (Filas de Mensagens)

## O que é o Amazon SQS?

**Amazon SQS (Simple Queue Service)** é o serviço de filas de mensagens da AWS.
Permite que componentes de um sistema se comuniquem de forma **assíncrona e desacoplada**.

### Analogia do Mundo Real
Imagine um restaurante:
- **Sem fila**: Garçom vai até a cozinha e espera o prato ficar pronto (bloqueante)
- **Com fila (SQS)**: Garçom anota o pedido e passa para a cozinha, vai atender outra mesa
  (assíncrono, desacoplado)

### Por que usar SQS?

```
SEM SQS:
  Sistema de Vendas → (chamada direta) → Glue ETL
  Se o Glue estiver ocupado: ERRO! Venda perdida.
  Se o Glue cair: ERRO! Venda perdida.

COM SQS:
  Sistema de Vendas → [mensagem na fila] → (quando disponível) → Glue ETL
  Se o Glue estiver ocupado: mensagem fica na fila (aguarda)
  Se o Glue cair: mensagem fica na fila (retry automático)
  Resultado: ZERO mensagens perdidas!
```

## Tipos de Filas SQS

### 1. Standard Queue (Fila Padrão)
- **Throughput**: Quase ilimitado (tráfego alto)
- **Ordenação**: Melhor esforço (não garante ordem)
- **Entrega**: Pelo menos uma vez (pode duplicar)
- **Custo**: $0.40/1M mensagens

### 2. FIFO Queue (First-In-First-Out)
- **Throughput**: 300 TPS (300 mensagens/segundo)
- **Ordenação**: Garantida (estritamente em ordem)
- **Entrega**: Exatamente uma vez (sem duplicatas)
- **Custo**: $0.50/1M mensagens
- **Uso**: Transações financeiras, pedidos de e-commerce

## Conceitos Importantes

### Visibility Timeout (Tempo de Visibilidade)
Quando um consumer lê uma mensagem, ela fica "invisível" para outros consumers.
Se o consumer não confirmar o processamento no tempo limite, a mensagem reaparece.

```
Producer → [mensagem] → Fila
Consumer A lê mensagem → Visibility Timeout: 30s
├── Se processou OK em 30s: deleta a mensagem (success)
└── Se não processou em 30s: mensagem reaparece para outro consumer (retry)
```

### Dead Letter Queue (DLQ)
Fila especial para mensagens que falharam múltiplas vezes:
```
Fila Principal → Consumer → FALHA
→ retry 1 → FALHA
→ retry 2 → FALHA
→ retry 3 → Move para DLQ (Dead Letter Queue)
```
O time de engenharia analisa as mensagens na DLQ para investigar o problema.

### Long Polling vs Short Polling
- **Short Polling**: Consumer verifica a fila e retorna imediatamente (mesmo vazia)
  - Problema: muitas chamadas desnecessárias (custo + latência)
- **Long Polling**: Consumer espera até 20 segundos por mensagens
  - Recomendado: reduz chamadas e custo

## Estrutura deste Projeto

```
sqs/
├── queues/
│   ├── vendas_queue.json      # Configuração da fila principal de vendas
│   └── dlq_errors.json        # Configuração da Dead Letter Queue
└── sqs_local.py               # Implementação local do SQS (usando arquivos JSON)
```

## Equivalência: SQS Local vs AWS Real

```python
# AWS REAL (boto3):
import boto3
sqs = boto3.client('sqs', region_name='us-east-1')

# Criar fila
response = sqs.create_queue(QueueName='vendas-queue')
queue_url = response['QueueUrl']

# Enviar mensagem
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody=json.dumps({'id_venda': 123, 'valor': 599.90})
)

# Receber mensagens
messages = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=10,
    WaitTimeSeconds=20  # Long polling
)
```

```python
# SIMULAÇÃO LOCAL (este projeto):
from sqs.sqs_local import SQSLocal
sqs = SQSLocal()

# Enviar mensagem (mesma interface!)
sqs.send_message('vendas_queue', {'id_venda': 123, 'valor': 599.90})

# Receber mensagens
messages = sqs.receive_messages('vendas_queue', max_messages=10)
```
