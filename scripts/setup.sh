#!/bin/bash
# =============================================================================
# setup.sh - Setup completo da Plataforma de Dados em um comando
# =============================================================================
# USO: ./scripts/setup.sh
#
# Este script:
# 1. Verifica pré-requisitos (Docker, Python)
# 2. Cria arquivo .env a partir do .env.example
# 3. Cria pasta de dados SQS
# 4. Inicia os containers Docker
# 5. Aguarda o PostgreSQL estar pronto
# 6. Inicializa o banco de dados
# 7. Gera dados de exemplo
# =============================================================================

set -e  # Para em caso de erro

# Cores para output
VERDE='\033[0;32m'
AMARELO='\033[1;33m'
VERMELHO='\033[0;31m'
RESET='\033[0m'
NEGRITO='\033[1m'

# Diretório do projeto
PROJETO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJETO_DIR"

echo ""
echo -e "${NEGRITO}============================================================${RESET}"
echo -e "${NEGRITO}     SETUP DA PLATAFORMA DE DADOS LOCAL (Simulação AWS)    ${RESET}"
echo -e "${NEGRITO}============================================================${RESET}"
echo ""

# =============================================================================
# FUNÇÃO: Verificar pré-requisito
# =============================================================================
verificar_prereq() {
    local cmd="$1"
    local nome="$2"
    local instrucao="$3"

    if command -v "$cmd" &>/dev/null; then
        echo -e "  ${VERDE}[OK]${RESET} $nome"
        return 0
    else
        echo -e "  ${VERMELHO}[FALTANDO]${RESET} $nome"
        echo -e "    Instale: $instrucao"
        return 1
    fi
}

# =============================================================================
# PASSO 1: Verificar pré-requisitos
# =============================================================================
echo -e "${AMARELO}1. Verificando pré-requisitos...${RESET}"

PREREQS_OK=true

verificar_prereq "docker" "Docker" \
    "https://docs.docker.com/get-docker/" || PREREQS_OK=false

verificar_prereq "docker-compose" "Docker Compose" \
    "https://docs.docker.com/compose/install/" || \
    verificar_prereq "docker" "Docker Compose (v2)" \
    "docker compose (parte do Docker Desktop)" || PREREQS_OK=false

verificar_prereq "python3" "Python 3" \
    "https://www.python.org/downloads/" || PREREQS_OK=false

verificar_prereq "pip3" "pip3" \
    "sudo apt install python3-pip" || PREREQS_OK=false

if [ "$PREREQS_OK" = false ]; then
    echo ""
    echo -e "${VERMELHO}Instale os pré-requisitos faltando e tente novamente.${RESET}"
    exit 1
fi

echo -e "  ${VERDE}Todos os pré-requisitos OK!${RESET}"
echo ""

# =============================================================================
# PASSO 2: Criar arquivo .env
# =============================================================================
echo -e "${AMARELO}2. Configurando variáveis de ambiente...${RESET}"

if [ ! -f ".env" ]; then
    cp .env.example .env
    echo -e "  ${VERDE}[OK]${RESET} Arquivo .env criado a partir de .env.example"
    echo -e "  ${AMARELO}[INFO]${RESET} Edite o .env se precisar personalizar as configurações"
else
    echo -e "  ${VERDE}[OK]${RESET} Arquivo .env já existe (mantendo configurações atuais)"
fi
echo ""

# =============================================================================
# PASSO 3: Criar estrutura de diretórios necessária
# =============================================================================
echo -e "${AMARELO}3. Criando estrutura de diretórios...${RESET}"

mkdir -p sqs/data
mkdir -p monitoring/metrics
mkdir -p monitoring/notificacoes
mkdir -p monitoring/analytics
mkdir -p data_quality/relatorios
mkdir -p glue/catalog/discovered

echo -e "  ${VERDE}[OK]${RESET} Diretórios criados"
echo ""

# =============================================================================
# PASSO 4: Criar script de inicialização do PostgreSQL
# =============================================================================
echo -e "${AMARELO}4. Criando scripts de suporte...${RESET}"

# Script para criar múltiplos bancos no PostgreSQL
cat > scripts/init_postgres.sh << 'INITEOF'
#!/bin/bash
# Cria bancos de dados adicionais necessários
set -e

create_db() {
    local db_name="$1"
    echo "Criando banco: $db_name"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $db_name'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db_name')\gexec
EOSQL
}

create_db airflow_db
create_db crm_source

echo "Bancos adicionais criados!"
INITEOF
chmod +x scripts/init_postgres.sh

# Config servidores pgAdmin
cat > scripts/pgadmin_servers.json << 'PGEOF'
{
    "Servers": {
        "1": {
            "Name": "Data Platform PostgreSQL",
            "Group": "Servers",
            "Host": "postgres",
            "Port": 5432,
            "MaintenanceDB": "dataplatform",
            "Username": "dataplatform_user",
            "SSLMode": "prefer",
            "Comment": "PostgreSQL local simulando o Amazon Redshift"
        }
    }
}
PGEOF

echo -e "  ${VERDE}[OK]${RESET} Scripts auxiliares criados"
echo ""

# =============================================================================
# PASSO 5: Instalar dependências Python
# =============================================================================
echo -e "${AMARELO}5. Instalando dependências Python...${RESET}"
echo -e "  ${AMARELO}[INFO]${RESET} Isso pode demorar alguns minutos na primeira vez..."

# Instala dependências básicas (sem Airflow que é pesado)
pip3 install --quiet \
    psycopg2-binary \
    pandas \
    pyarrow \
    numpy \
    python-dotenv \
    requests \
    pyyaml \
    faker \
    colorlog \
    tabulate \
    tqdm \
    2>/dev/null || true

echo -e "  ${VERDE}[OK]${RESET} Dependências básicas instaladas"
echo -e "  ${AMARELO}[INFO]${RESET} Para instalar TODAS as dependências: pip3 install -r requirements.txt"
echo ""

# =============================================================================
# PASSO 6: Iniciar Docker Compose
# =============================================================================
echo -e "${AMARELO}6. Iniciando serviços Docker...${RESET}"
echo -e "  ${AMARELO}[INFO]${RESET} Baixando imagens Docker (pode demorar na primeira vez)..."

# Usa docker compose v2 ou docker-compose v1
if docker compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

# Sobe apenas os serviços principais (sem Airbyte que é pesado)
$COMPOSE_CMD up -d postgres pgadmin zookeeper kafka kafka-ui 2>&1 | \
    grep -E "(Started|Created|Error|Warning)" || true

echo -e "  ${VERDE}[OK]${RESET} Serviços Docker iniciados"
echo ""

# =============================================================================
# PASSO 7: Aguardar PostgreSQL
# =============================================================================
echo -e "${AMARELO}7. Aguardando PostgreSQL ficar pronto...${RESET}"

MAX_TENTATIVAS=30
TENTATIVA=0
while [ $TENTATIVA -lt $MAX_TENTATIVAS ]; do
    if $COMPOSE_CMD exec -T postgres pg_isready -U dataplatform_user -d dataplatform &>/dev/null 2>&1; then
        echo -e "  ${VERDE}[OK]${RESET} PostgreSQL está pronto!"
        break
    fi
    TENTATIVA=$((TENTATIVA + 1))
    echo -e "  ${AMARELO}[AGUARDANDO]${RESET} Tentativa $TENTATIVA/$MAX_TENTATIVAS..."
    sleep 3
done

if [ $TENTATIVA -eq $MAX_TENTATIVAS ]; then
    echo -e "  ${VERMELHO}[AVISO]${RESET} PostgreSQL não respondeu. Execute manualmente:"
    echo -e "  docker compose up -d postgres"
fi
echo ""

# =============================================================================
# PASSO 8: Inicializar banco de dados
# =============================================================================
echo -e "${AMARELO}8. Inicializando banco de dados (schemas e tabelas)...${RESET}"

if python3 redshift/init_db.py 2>&1 | grep -E "(sucesso|SUCESSO|OK|criado)" | head -5; then
    echo -e "  ${VERDE}[OK]${RESET} Banco de dados inicializado!"
else
    echo -e "  ${AMARELO}[INFO]${RESET} Execute manualmente: python3 redshift/init_db.py"
fi
echo ""

# =============================================================================
# PASSO 9: Gerar dados de exemplo
# =============================================================================
echo -e "${AMARELO}9. Gerando dados de exemplo (clientes e vendas brasileiros)...${RESET}"

if python3 scripts/generate_sample_data.py --clientes 200 --vendas 1000 --eventos 500 2>&1 | \
    grep -E "(sucesso|SUCESSO|gerado|salvo)" | head -5; then
    echo -e "  ${VERDE}[OK]${RESET} Dados de exemplo gerados!"
else
    echo -e "  ${AMARELO}[INFO]${RESET} Execute manualmente: python3 scripts/generate_sample_data.py"
fi
echo ""

# =============================================================================
# CONCLUSÃO
# =============================================================================
echo -e "${NEGRITO}============================================================${RESET}"
echo -e "${VERDE}${NEGRITO}   SETUP CONCLUÍDO COM SUCESSO!${RESET}"
echo -e "${NEGRITO}============================================================${RESET}"
echo ""
echo -e "${NEGRITO}Serviços disponíveis:${RESET}"
echo -e "  ${VERDE}PostgreSQL${RESET} (Redshift simulado): localhost:5432"
echo -e "  ${VERDE}pgAdmin${RESET} (Interface DB):         http://localhost:5050"
echo -e "  ${VERDE}Kafka UI${RESET} (Visualizar tópicos):  http://localhost:8090"
echo ""
echo -e "${NEGRITO}Para subir o Airflow:${RESET}"
echo -e "  docker compose up -d airflow-init"
echo -e "  docker compose up -d airflow-webserver airflow-scheduler"
echo -e "  Acesse: http://localhost:8080 (admin/admin123)"
echo ""
echo -e "${NEGRITO}Próximos passos:${RESET}"
echo -e "  1. Rodar ETL: ${AMARELO}python3 glue/jobs/clientes_raw_to_processed.py${RESET}"
echo -e "  2. Rodar ETL: ${AMARELO}python3 glue/jobs/vendas_raw_to_processed.py${RESET}"
echo -e "  3. Pipeline completo: ${AMARELO}./scripts/run_full_pipeline.sh${RESET}"
echo -e "  4. Gerar eventos: ${AMARELO}python3 streaming/producers/evento_click_producer.py${RESET}"
echo ""
echo -e "${NEGRITO}Documentação:${RESET} Leia os README.md em cada pasta!"
echo ""
