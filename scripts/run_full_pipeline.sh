#!/bin/bash
# =============================================================================
# run_full_pipeline.sh - Executa o pipeline completo end-to-end
# =============================================================================
# Simula a execução do DAG dag_etl_full_pipeline sem o Airflow.
# Útil para testar e depurar o pipeline manualmente.
#
# USO: ./scripts/run_full_pipeline.sh
# =============================================================================

set -e

VERDE='\033[0;32m'
AMARELO='\033[1;33m'
VERMELHO='\033[0;31m'
RESET='\033[0m'
NEGRITO='\033[1m'

PROJETO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJETO_DIR"

DATA_HOJE=$(date +%Y-%m-%d)
INICIO=$(date +%s)

echo ""
echo -e "${NEGRITO}============================================================${RESET}"
echo -e "${NEGRITO}      PIPELINE COMPLETO - DATA: $DATA_HOJE                 ${RESET}"
echo -e "${NEGRITO}============================================================${RESET}"
echo ""

passo_ok() {
    echo -e "  ${VERDE}[OK]${RESET} $1"
}

passo_erro() {
    echo -e "  ${VERMELHO}[ERRO]${RESET} $1"
}

executar_passo() {
    local nome="$1"
    local cmd="$2"

    echo -e "${AMARELO}>>> $nome${RESET}"
    if eval "$cmd" > /tmp/pipeline_step.log 2>&1; then
        passo_ok "$nome concluído"
        return 0
    else
        passo_erro "$nome FALHOU"
        echo "  Log: $(tail -3 /tmp/pipeline_step.log)"
        return 1
    fi
}

# PASSO 1: Verificar dados disponíveis
echo -e "${AMARELO}1. Verificando dados disponíveis...${RESET}"
CLIENTES_RAW=$(find s3/raw/clientes -name "*.json" 2>/dev/null | wc -l)
VENDAS_RAW=$(find s3/raw/vendas -name "*.csv" 2>/dev/null | wc -l)
echo "   Arquivos raw clientes: $CLIENTES_RAW"
echo "   Arquivos raw vendas  : $VENDAS_RAW"

if [ "$CLIENTES_RAW" -eq 0 ] || [ "$VENDAS_RAW" -eq 0 ]; then
    echo -e "  ${AMARELO}Gerando dados de exemplo...${RESET}"
    python3 scripts/generate_sample_data.py --clientes 200 --vendas 1000 --eventos 200
fi
echo ""

# PASSO 2: Validação de schema (Lambda)
executar_passo "Validação de Schema (Lambda)" \
    "python3 -c \"
import sys; sys.path.insert(0, '.')
from lambda.functions.validate_schema import lambda_handler
import os, glob
arquivos = glob.glob('s3/raw/clientes/**/*.json', recursive=True)[:2]
for f in arquivos:
    evento = {'Records': [{'s3': {'bucket': {'name': 'local'}, 'object': {'key': f, 'size': os.path.getsize(f)}}}]}
    r = lambda_handler(evento)
    print(f'  {f}: status={r[\\\"statusCode\\\"]}')
\"" || true
echo ""

# PASSO 3: ETL Glue - Clientes
executar_passo "Glue Job: Clientes (raw → processed)" \
    "python3 glue/jobs/clientes_raw_to_processed.py"
echo ""

# PASSO 4: ETL Glue - Vendas
executar_passo "Glue Job: Vendas (raw → processed)" \
    "python3 glue/jobs/vendas_raw_to_processed.py"
echo ""

# PASSO 5: Agregações
executar_passo "Glue Job: Agregações Diárias" \
    "python3 glue/jobs/daily_aggregations.py --data $DATA_HOJE" || true
echo ""

# PASSO 6: Data Quality
executar_passo "Data Quality: Clientes" \
    "python3 data_quality/checks/dq_clientes.py" || true

executar_passo "Data Quality: Vendas" \
    "python3 data_quality/checks/dq_vendas.py" || true
echo ""

# PASSO 7: Analytics de streaming
executar_passo "Analytics de Eventos (Streaming)" \
    "python3 streaming/consumers/consumer_analytics.py" || true
echo ""

# PASSO 8: Resumo
FIM=$(date +%s)
DURACAO=$((FIM - INICIO))

echo -e "${NEGRITO}============================================================${RESET}"
echo -e "${VERDE}${NEGRITO}   PIPELINE CONCLUÍDO!${RESET}"
echo -e "${NEGRITO}============================================================${RESET}"
echo ""
echo -e "Duração total: ${DURACAO}s"
echo ""
echo -e "${NEGRITO}Arquivos gerados:${RESET}"
PROC_CLIENTES=$(find s3/processed/clientes -name "*.parquet" 2>/dev/null | wc -l)
PROC_VENDAS=$(find s3/processed/vendas -name "*.parquet" 2>/dev/null | wc -l)
echo -e "  s3/processed/clientes/: $PROC_CLIENTES arquivos Parquet"
echo -e "  s3/processed/vendas/  : $PROC_VENDAS arquivos Parquet"
echo ""
echo -e "${NEGRITO}Para ver os dados no banco:${RESET}"
echo -e "  Acesse pgAdmin: http://localhost:5050"
echo -e "  Ou: python3 redshift/init_db.py"
echo ""
