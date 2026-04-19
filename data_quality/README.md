# Data Quality - Qualidade de Dados com Great Expectations

## Por que Qualidade de Dados Importa?

"Lixo entra, lixo sai" (Garbage In, Garbage Out)

Sem validação de qualidade:
- Dashboard mostra receita R$0 (campo valor nulo)
- Pipeline duplica clientes (sem deduplicação)
- BI reporta crescimento falso (dados duplicados)

Com Great Expectations:
- Valida ANTES de processar (falha cedo, não tarde)
- Gera documentação automática do que é esperado
- Alertas quando qualidade cai

## Great Expectations vs AWS Glue Data Quality

| Aspecto           | Great Expectations (local) | AWS Glue Data Quality      |
|-------------------|----------------------------|----------------------------|
| Tipo              | Open-source Python         | Serviço gerenciado AWS     |
| Integração        | Qualquer pipeline          | Nativo com Glue ETL        |
| Custo             | Grátis                     | $0.25/DPU-hora             |
| Interface visual  | Data Docs (HTML)           | Console AWS                |
| Regras            | Python/YAML                | DQDL (linguagem própria)   |

## Conceitos Great Expectations

### Expectation Suite
Conjunto de regras de validação para uma tabela:
```python
suite = context.create_expectation_suite("clientes_suite")
```

### Expectations (Expectativas)
Regras individuais:
```python
# Coluna CPF não pode ter nulos
batch.expect_column_values_to_not_be_null('cpf')

# CPF deve ter exatamente 11 caracteres
batch.expect_column_value_lengths_to_equal('cpf', 11)

# Email deve seguir formato de email
batch.expect_column_values_to_match_regex('email', r'^[^@]+@[^@]+\.[^@]+$')

# Valor total deve ser positivo
batch.expect_column_values_to_be_between('valor_total', min_value=0.01)

# Não deve ter duplicatas por CPF
batch.expect_column_values_to_be_unique('cpf')
```

### Checkpoint
Executa as validações e gera relatório:
```python
checkpoint.run()  # Gera HTML com resultado
```

## Executando Localmente

```bash
# Valida dados de clientes
python data_quality/checks/dq_clientes.py

# Valida dados de vendas
python data_quality/checks/dq_vendas.py
```
