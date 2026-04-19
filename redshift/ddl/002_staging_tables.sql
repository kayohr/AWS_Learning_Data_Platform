-- =============================================================================
-- 002_staging_tables.sql - Tabelas de Staging (zona de aterrissagem)
-- =============================================================================
-- CONCEITO AWS REDSHIFT - STAGING:
--   As tabelas de staging recebem dados diretamente do S3 via comando COPY.
--   São truncadas e recarregadas em cada execução (padrão: full refresh).
--
--   Fluxo:
--     S3 processed/ → [COPY] → staging.stg_clientes → [SP] → dw.dim_clientes
--
--   DISTKEY ALL: tabelas de staging geralmente são pequenas e usadas em JOINs,
--   por isso ALL (replicar em todos os nós) é mais eficiente.
--
-- NOTA POSTGRESQL LOCAL:
--   DISTKEY e ENCODE são ignorados pelo PostgreSQL.
--   São comentários informativos sobre como seria no Redshift real.
-- =============================================================================

-- =============================================================================
-- TABELA: staging.stg_clientes
-- =============================================================================
DROP TABLE IF EXISTS staging.stg_clientes CASCADE;

CREATE TABLE staging.stg_clientes (
    -- -------------------------------------------------------------------------
    -- Campos de negócio (vêm do S3 processed/clientes/)
    -- -------------------------------------------------------------------------
    id              BIGINT,                     -- ID do cliente no CRM
    cpf             VARCHAR(11)     NOT NULL,   -- CPF sem formatação (11 dígitos)
    nome            VARCHAR(200)    NOT NULL,   -- Nome completo normalizado
    email           VARCHAR(200),               -- E-mail (pode ser nulo se inválido)
    telefone        VARCHAR(20),                -- Telefone formatado
    data_nascimento DATE,                       -- Data de nascimento
    data_cadastro   TIMESTAMP,                  -- Quando cadastrou no CRM
    cidade          VARCHAR(100),               -- Cidade de residência
    uf              CHAR(2),                    -- Estado (SP, RJ, MG, ...)
    cep             VARCHAR(8),                 -- CEP sem formatação
    segmento_cliente VARCHAR(10),               -- B2B ou B2C
    limite_credito  DECIMAL(15,2),              -- Limite de crédito em R$
    ativo           BOOLEAN         DEFAULT TRUE, -- Cliente ativo?

    -- -------------------------------------------------------------------------
    -- Campos de controle/auditoria (adicionados pelo job Glue)
    -- -------------------------------------------------------------------------
    _source_file         VARCHAR(500),    -- Arquivo S3 de origem
    _ingestion_timestamp TIMESTAMP,       -- Quando foi ingerido
    _processed_at        TIMESTAMP,       -- Quando foi transformado pelo Glue

    -- -------------------------------------------------------------------------
    -- Campos de controle do staging
    -- -------------------------------------------------------------------------
    stg_load_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_batch_id         VARCHAR(50)      -- ID do batch de carga (para auditoria)

    -- REDSHIFT REAL: adicionaria
    -- DISTKEY (cpf)     -- Distribui por CPF para JOINs eficientes
    -- SORTKEY (data_cadastro)  -- Ordena por data para range queries
);

-- Comentário da tabela (no Redshift, usa COMMENT ON TABLE)
COMMENT ON TABLE staging.stg_clientes IS
    'Staging: dados brutos de clientes vindos do S3 processed/clientes/. '
    'Truncado e recarregado a cada execução do pipeline.';

COMMENT ON COLUMN staging.stg_clientes.cpf IS
    'CPF sem formatação (11 dígitos numéricos). Chave de negócio dos clientes.';

COMMENT ON COLUMN staging.stg_clientes.stg_batch_id IS
    'ID do batch de carga. Permite rastrear qual execução do pipeline carregou este registro.';

-- =============================================================================
-- TABELA: staging.stg_vendas
-- =============================================================================
DROP TABLE IF EXISTS staging.stg_vendas CASCADE;

CREATE TABLE staging.stg_vendas (
    -- Identificadores
    id_venda        BIGINT,
    id_pedido       VARCHAR(50),        -- Número do pedido no ERP

    -- Referências (chaves estrangeiras para JOINs)
    cpf_cliente     VARCHAR(11),        -- Chave para buscar dim_clientes
    produto_id      VARCHAR(50),        -- Chave para buscar dim_produto
    vendedor_id     VARCHAR(50),        -- Vendedor responsável
    loja_id         VARCHAR(50),        -- Loja física (se aplicável)

    -- Datas
    data_venda      TIMESTAMP,          -- Data e hora da venda
    data_entrega    TIMESTAMP,          -- Data de entrega
    data_cancelamento TIMESTAMP,        -- Data de cancelamento (se cancelado)

    -- Produto
    produto_nome    VARCHAR(300),       -- Nome do produto
    categoria_produto VARCHAR(100),     -- Categoria (Eletrônicos, Vestuário, etc.)
    quantidade      INTEGER,            -- Quantidade vendida

    -- Valores financeiros (em R$)
    valor_unitario  DECIMAL(15,2),      -- Valor de cada unidade
    desconto        DECIMAL(15,2),      -- Desconto aplicado
    valor_total     DECIMAL(15,2),      -- Valor total (qtd * unitário - desconto)
    custo           DECIMAL(15,2),      -- Custo do produto (para margem)

    -- Informações da venda
    canal_venda     VARCHAR(30),        -- LOJA_FISICA, ECOMMERCE, TELEVENDAS, APP_MOBILE
    status_venda    VARCHAR(20),        -- CONCLUIDA, CANCELADA, DEVOLVIDA, PENDENTE
    forma_pagamento VARCHAR(30),        -- CARTAO_CREDITO, PIX, BOLETO, etc.
    parcelas        INTEGER DEFAULT 1,  -- Número de parcelas

    -- Campos calculados pelo Glue
    categoria_valor VARCHAR(20),        -- PEQUENA, MEDIA, GRANDE, PREMIUM
    margem_pct      DECIMAL(10,4),      -- Margem de lucro em %
    ano_venda       INTEGER,            -- Ano da venda (para particionamento)
    mes_venda       INTEGER,            -- Mês da venda (para particionamento)
    dia_semana      VARCHAR(20),        -- Nome do dia da semana

    -- Auditoria
    _source_file         VARCHAR(500),
    _ingestion_timestamp TIMESTAMP,
    _processed_at        TIMESTAMP,

    -- Controle staging
    stg_load_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_batch_id         VARCHAR(50)

    -- REDSHIFT REAL:
    -- DISTKEY (cpf_cliente)
    -- SORTKEY (data_venda)
);

COMMENT ON TABLE staging.stg_vendas IS
    'Staging: dados de vendas vindos do S3 processed/vendas/. '
    'Pode ser incremental (apenas novas vendas) ou full refresh.';

-- =============================================================================
-- ÍNDICES para otimizar JOINs no staging
-- (No Redshift: SORTKEY e DISTKEY fazem este papel)
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_stg_clientes_cpf
    ON staging.stg_clientes(cpf);

CREATE INDEX IF NOT EXISTS idx_stg_vendas_cpf
    ON staging.stg_vendas(cpf_cliente);

CREATE INDEX IF NOT EXISTS idx_stg_vendas_data
    ON staging.stg_vendas(data_venda);

-- =============================================================================
-- VIEW para facilitar verificação do staging
-- =============================================================================
CREATE OR REPLACE VIEW staging.vw_staging_resumo AS
SELECT
    'stg_clientes' AS tabela,
    COUNT(*) AS total_registros,
    MAX(stg_load_timestamp) AS ultima_carga,
    MIN(stg_load_timestamp) AS primeira_carga
FROM staging.stg_clientes
UNION ALL
SELECT
    'stg_vendas' AS tabela,
    COUNT(*) AS total_registros,
    MAX(stg_load_timestamp) AS ultima_carga,
    MIN(stg_load_timestamp) AS primeira_carga
FROM staging.stg_vendas;

COMMENT ON VIEW staging.vw_staging_resumo IS
    'Visão de resumo do staging: quantos registros tem em cada tabela.';

-- Confirmação
DO $$
BEGIN
    RAISE NOTICE 'Tabelas de staging criadas: stg_clientes, stg_vendas';
    RAISE NOTICE 'Index criados para otimizar JOINs';
END $$;
