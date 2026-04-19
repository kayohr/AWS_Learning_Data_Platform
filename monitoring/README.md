# Monitoring - Monitoramento da Plataforma

## AWS CloudWatch (Equivalente Local)

No AWS, CloudWatch monitora tudo automaticamente:
- **Metrics**: CPU, memória, erros, latência
- **Logs**: Logs de todos os serviços centralizados
- **Alarms**: Alertas quando métricas excedem limiares
- **Dashboards**: Visualização em tempo real

Localmente:
- **Metrics**: Arquivos JSON em monitoring/metrics/
- **Logs**: Arquivos de log dos scripts Python
- **Alarms**: Verificações no arquivo alert_rules.json
- **Dashboards**: Configuração Grafana em pipeline_health.json

## O que Monitorar numa Plataforma de Dados

```
DADOS:
  - Freshness: "quando foi a última atualização?"
  - Volume: "a contagem de linhas caiu muito?"
  - Qualidade: "quantos registros inválidos?"

PIPELINE:
  - Duração: "o job está demorando mais que o normal?"
  - Falhas: "houve erro? qual a causa raiz?"
  - SLA: "o pipeline terminou antes das 06:00?"

INFRAESTRUTURA:
  - CPU/Memória dos workers
  - Tamanho das filas SQS
  - Espaço em disco (S3 local)
```

## Arquivos

- `alerts/alert_rules.json`: Regras de alertas
- `dashboards/pipeline_health.json`: Config do dashboard
