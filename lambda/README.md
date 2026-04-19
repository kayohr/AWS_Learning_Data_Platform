# Lambda - Simulação do AWS Lambda (Funções Serverless)

## O que é o AWS Lambda?

**AWS Lambda** é o serviço de computação serverless da AWS.
Você escreve funções que rodam **em resposta a eventos**, sem gerenciar servidores.

### Características Principais

- **Serverless**: Zero gerenciamento de infraestrutura
- **Event-driven**: Funções são disparadas por eventos
- **Pay per use**: Paga apenas pelo tempo de execução (nunca por servidor ocioso)
- **Escala automática**: De 0 a milhares de execuções simultâneas em segundos

### Modelos de Precificação Lambda
- **Requests**: $0.20 por 1 milhão de requisições
- **Duration**: $0.0000166667 por GB-segundo
- **Exemplo**: 1 milhão de execuções de 100ms com 128MB = ~$0.21/mês

## Triggers (Gatilhos) do Lambda

O Lambda pode ser disparado por diversos eventos AWS:

```
╔══════════════════╗     evento      ╔══════════════════╗
║   GATILHO        ║ ─────────────→ ║  FUNÇÃO LAMBDA   ║
╠══════════════════╣                 ╠══════════════════╣
║ S3 Event         ║                 ║ validate_schema  ║
║ (novo arquivo)   ║                 ║ notify_pipeline  ║
╠══════════════════╣                 ║ archive_old_data ║
║ SQS Message      ║                 ╚══════════════════╝
║ (nova mensagem)  ║
╠══════════════════╣
║ API Gateway      ║
║ (HTTP request)   ║
╠══════════════════╣
║ EventBridge      ║
║ (cron schedule)  ║
╚══════════════════╝
```

## Funções deste Projeto

### 1. validate_schema.py
Dispara quando um novo arquivo chega no S3 raw/.
Verifica se o arquivo tem as colunas obrigatórias.
Se inválido, move para uma pasta de quarentena.

### 2. notify_pipeline.py
Dispara quando um pipeline falha.
Envia notificação via e-mail/Slack.
No AWS: usaria SNS (Simple Notification Service).

### 3. archive_old_data.py
Dispara por agendamento (EventBridge/cron).
Move arquivos com mais de 90 dias para archived/.

## Evento S3 → Lambda

Quando um arquivo é criado no S3, a AWS envia um evento assim:
```json
{
    "Records": [{
        "eventName": "ObjectCreated:Put",
        "s3": {
            "bucket": {"name": "minha-empresa-data-lake"},
            "object": {
                "key": "raw/clientes/2024/01/15/clientes.json",
                "size": 15234
            }
        }
    }]
}
```
O Lambda recebe esse JSON e sabe qual arquivo processar.

## Simulação Local

Como não temos AWS aqui, simulamos os triggers:

1. **S3 Event**: Um script Python monitora a pasta s3/raw/ e dispara a função
2. **SQS Event**: Lê mensagens do arquivo queue.json e processa
3. **Agendamento**: O Airflow chama as funções Lambda simuladas

## Cold Start - Conceito Importante

O **Cold Start** acontece quando o Lambda não tem instâncias "quentes":
```
1ª execução: 500-2000ms (inicializa o container)
2ª execução:   50-100ms (container já está quente)
3ª execução:   50-100ms (idem)
...
Após 15min de inatividade: novo cold start
```

Para mitigar: **Provisioned Concurrency** (mantém instâncias sempre quentes, mas custa mais).

## Limites do Lambda

| Recurso         | Limite        | Observação                |
|-----------------|---------------|---------------------------|
| Timeout máximo  | 15 minutos    | Use Step Functions para mais |
| Memória         | 128MB - 10GB  | Mais memória = mais CPU   |
| Tamanho do pacote | 50MB (zip)  | 250MB descomprimido       |
| Concurrent executions | 1000  | Pode aumentar via quota   |

Para processar arquivos grandes (>15min), use **AWS Batch** ou **Glue Jobs**.
