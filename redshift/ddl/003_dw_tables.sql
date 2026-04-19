-- =============================================================================
-- 003_dw_tables.sql - Tabelas do Data Warehouse (Star Schema)
-- =============================================================================
-- CONCEITO STAR SCHEMA (Esquema Estrela):
--   O modelo dimensional é o padrão para Data Warehouses analíticos.
--   Proposto por Ralph Kimball (The Data Warehouse Toolkit).
--
--   Componentes:
--     DIMENSÕES (dim_*): Tabelas descritivas com atributos de análise
--       "Quem?" → dim_clientes
--       "O quê?" → dim_produto
--       "Quando?" → dim_tempo (dimensão de calendário)
--
--     FATOS (fato_*): Tabelas com métricas e medidas numéricas
--       "Quanto?" → fato_vendas (valores, quantidades)
--
--   Vantagens:
--     1. Queries analíticas simples e rápidas
--     2. Fácil de entender para analistas de negócio
--     3. Otimizado para GROUP BY e agregações
--     4. Histórico preservado (SCD - Slowly Changing Dimensions)
-- =============================================================================

-- =============================================================================
-- DIMENSÃO: dw.dim_clientes
-- =============================================================================
-- Armazena TODOS os atributos descritivos dos clientes.
-- Quando um dado do cliente muda (ex: cidade), guardamos o histórico (SCD Tipo 2).
--
-- SCD Tipo 2 (Slowly Changing Dimension Type 2):
--   Quando um cliente muda de cidade:
--   Antes: id_cliente=1, cidade=SP, data_inicio=2020-01-01, data_fim=NULL, ativo=TRUE
--   Depois: insere novo registro:
--           id_cliente=1, cidade=RJ, data_inicio=2024-01-15, data_fim=NULL, ativo=TRUE
--   E atualiza o antigo:
--           id_cliente=1, cidade=SP, data_inicio=2020-01-01, data_fim=2024-01-14, ativo=FALSE
-- =============================================================================
DROP TABLE IF EXISTS dw.dim_clientes CASCADE;

CREATE TABLE dw.dim_clientes (
    -- Surrogate Key: chave sintética gerada pelo DW (não vem da fonte)
    -- No Redshift real: IDENTITY(1,1) gera automaticamente
    sk_cliente      BIGSERIAL       PRIMARY KEY,

    -- Natural Key: chave de negócio (vem do sistema de origem)
    cpf             VARCHAR(11)     NOT NULL,       -- Chave de negócio

    -- Atributos do cliente
    nome            VARCHAR(200)    NOT NULL,
    email           VARCHAR(200),
    telefone        VARCHAR(20),
    data_nascimento DATE,
    data_cadastro_crm TIMESTAMP,                    -- Quando entrou no CRM

    -- Localização
    cidade          VARCHAR(100),
    uf              CHAR(2),
    cep             VARCHAR(8),
    regiao          VARCHAR(20),                    -- N, NE, CO, SE, S (derivado da UF)

    -- Segmentação
    segmento_cliente VARCHAR(10),                   -- B2B ou B2C
    faixa_etaria    VARCHAR(20),                    -- Calculado da data_nascimento

    -- Limite de crédito
    limite_credito  DECIMAL(15,2),

    -- ==========================================================================
    -- CAMPOS SCD Tipo 2 (Slowly Changing Dimension)
    -- Permitem rastrear mudanças históricas
    -- ==========================================================================
    data_inicio_vigencia DATE    NOT NULL DEFAULT CURRENT_DATE,
    data_fim_vigencia    DATE,                      -- NULL = registro atual
    registro_atual       BOOLEAN NOT NULL DEFAULT TRUE,

    -- Auditoria DW
    dw_criado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_source       VARCHAR(50) DEFAULT 'staging.stg_clientes'

    -- REDSHIFT REAL:
    -- DISTKEY (cpf)     -- Distribui por CPF (mesmo que fato_vendas para JOIN eficiente)
    -- SORTKEY (sk_cliente) -- Ordena pela surrogate key
    -- ENCODE AUTO       -- Compressão automática por coluna
);

COMMENT ON TABLE dw.dim_clientes IS
    'Dimensão de clientes com SCD Tipo 2. '
    'Registros ativos têm registro_atual=TRUE e data_fim_vigencia=NULL.';

-- Calcula a região baseada na UF
CREATE OR REPLACE FUNCTION dw.fn_uf_para_regiao(uf CHAR(2))
RETURNS VARCHAR(20) AS $$
BEGIN
    RETURN CASE
        WHEN uf IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'NORTE'
        WHEN uf IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'NORDESTE'
        WHEN uf IN ('DF', 'GO', 'MS', 'MT') THEN 'CENTRO-OESTE'
        WHEN uf IN ('ES', 'MG', 'RJ', 'SP') THEN 'SUDESTE'
        WHEN uf IN ('PR', 'RS', 'SC') THEN 'SUL'
        ELSE 'DESCONHECIDO'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =============================================================================
-- DIMENSÃO: dw.dim_produto
-- =============================================================================
DROP TABLE IF EXISTS dw.dim_produto CASCADE;

CREATE TABLE dw.dim_produto (
    sk_produto      BIGSERIAL       PRIMARY KEY,
    produto_id      VARCHAR(50)     NOT NULL,       -- Chave de negócio
    produto_nome    VARCHAR(300)    NOT NULL,
    categoria       VARCHAR(100),
    subcategoria    VARCHAR(100),

    -- Classificação ABC de produtos
    -- A = top 20% dos produtos = 80% do faturamento (Pareto)
    -- B = próximos 30%
    -- C = últimos 50% (longa cauda)
    classificacao_abc VARCHAR(1),

    -- Status do produto
    ativo           BOOLEAN DEFAULT TRUE,

    -- Auditoria
    data_inicio_vigencia DATE    NOT NULL DEFAULT CURRENT_DATE,
    data_fim_vigencia    DATE,
    registro_atual       BOOLEAN NOT NULL DEFAULT TRUE,
    dw_criado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dw.dim_produto IS 'Dimensão de produtos com SCD Tipo 2.';

-- =============================================================================
-- DIMENSÃO: dw.dim_tempo
-- =============================================================================
-- A dimensão de tempo é pré-populada com datas futuras.
-- Isso permite JOINs eficientes sem cálculos de data nas queries.
--
-- Pergunta: Por que não usar DATE diretamente em fato_vendas?
-- Resposta: dim_tempo permite:
--   1. Adicionar atributos: feriado?, dia_util?, trimestre fiscal?
--   2. Queries mais simples: WHERE dim_tempo.dia_semana = 'SEGUNDA-FEIRA'
--   3. Performance: INTEGER (sk_tempo) é muito mais rápido que DATE
-- =============================================================================
DROP TABLE IF EXISTS dw.dim_tempo CASCADE;

CREATE TABLE dw.dim_tempo (
    sk_tempo            INTEGER         PRIMARY KEY,    -- Formato YYYYMMDD (ex: 20240115)
    data_completa       DATE            NOT NULL UNIQUE,
    ano                 SMALLINT        NOT NULL,
    mes                 SMALLINT        NOT NULL,       -- 1-12
    mes_nome            VARCHAR(20)     NOT NULL,       -- Janeiro, Fevereiro, ...
    mes_abrev           CHAR(3)         NOT NULL,       -- Jan, Fev, ...
    trimestre           SMALLINT        NOT NULL,       -- 1-4
    semestre            SMALLINT        NOT NULL,       -- 1-2
    semana_do_ano       SMALLINT        NOT NULL,       -- 1-53
    dia                 SMALLINT        NOT NULL,       -- 1-31
    dia_semana_num      SMALLINT        NOT NULL,       -- 1=DOM, 2=SEG, ... 7=SAB
    dia_semana_nome     VARCHAR(20)     NOT NULL,       -- Domingo, Segunda-Feira, ...
    dia_semana_abrev    CHAR(3)         NOT NULL,       -- Dom, Seg, ...
    fim_de_semana       BOOLEAN         NOT NULL,       -- TRUE se SAB ou DOM
    ultimo_dia_mes      BOOLEAN         NOT NULL,       -- TRUE se último dia do mês
    feriado_nacional    BOOLEAN         NOT NULL DEFAULT FALSE,
    feriado_nome        VARCHAR(100),                   -- Nome do feriado (se feriado)
    dia_util            BOOLEAN         NOT NULL,       -- Não feriado E não fim de semana

    -- Períodos fiscais (podem diferir do calendário)
    ano_fiscal          SMALLINT,                       -- Ano fiscal da empresa
    mes_fiscal          SMALLINT,                       -- Mês fiscal

    dw_criado_em        TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    -- REDSHIFT REAL:
    -- DISTSTYLE ALL   -- Tabela pequena, replique em todos os nós
    -- SORTKEY (data_completa)
);

COMMENT ON TABLE dw.dim_tempo IS
    'Dimensão de tempo pré-populada. sk_tempo = YYYYMMDD para JOINs eficientes.';

-- =============================================================================
-- FUNÇÃO AUXILIAR: Popula dim_tempo com um range de datas
-- =============================================================================
CREATE OR REPLACE FUNCTION dw.fn_popular_dim_tempo(
    data_inicio DATE,
    data_fim DATE
) RETURNS INTEGER AS $$
DECLARE
    data_atual DATE := data_inicio;
    contador INTEGER := 0;
    meses_pt TEXT[] := ARRAY['Janeiro','Fevereiro','Março','Abril','Maio','Junho',
                              'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'];
    meses_abrev TEXT[] := ARRAY['Jan','Fev','Mar','Abr','Mai','Jun',
                                 'Jul','Ago','Set','Out','Nov','Dez'];
    dias_semana TEXT[] := ARRAY['Domingo','Segunda-Feira','Terça-Feira','Quarta-Feira',
                                 'Quinta-Feira','Sexta-Feira','Sábado'];
    dias_abrev TEXT[] := ARRAY['Dom','Seg','Ter','Qua','Qui','Sex','Sab'];
BEGIN
    WHILE data_atual <= data_fim LOOP
        INSERT INTO dw.dim_tempo (
            sk_tempo, data_completa, ano, mes, mes_nome, mes_abrev,
            trimestre, semestre, semana_do_ano, dia,
            dia_semana_num, dia_semana_nome, dia_semana_abrev,
            fim_de_semana, ultimo_dia_mes, dia_util
        ) VALUES (
            TO_CHAR(data_atual, 'YYYYMMDD')::INTEGER,
            data_atual,
            EXTRACT(YEAR FROM data_atual)::SMALLINT,
            EXTRACT(MONTH FROM data_atual)::SMALLINT,
            meses_pt[EXTRACT(MONTH FROM data_atual)::INTEGER],
            meses_abrev[EXTRACT(MONTH FROM data_atual)::INTEGER],
            EXTRACT(QUARTER FROM data_atual)::SMALLINT,
            CASE WHEN EXTRACT(MONTH FROM data_atual) <= 6 THEN 1 ELSE 2 END::SMALLINT,
            EXTRACT(WEEK FROM data_atual)::SMALLINT,
            EXTRACT(DAY FROM data_atual)::SMALLINT,
            EXTRACT(DOW FROM data_atual)::SMALLINT + 1,
            dias_semana[EXTRACT(DOW FROM data_atual)::INTEGER + 1],
            dias_abrev[EXTRACT(DOW FROM data_atual)::INTEGER + 1],
            EXTRACT(DOW FROM data_atual) IN (0, 6),  -- DOM=0, SAB=6
            data_atual = (DATE_TRUNC('month', data_atual) + INTERVAL '1 month - 1 day')::DATE,
            EXTRACT(DOW FROM data_atual) NOT IN (0, 6)  -- dia_util = não fim de semana
        )
        ON CONFLICT (data_completa) DO NOTHING;  -- Evita duplicatas

        data_atual := data_atual + INTERVAL '1 day';
        contador := contador + 1;
    END LOOP;

    RETURN contador;
END;
$$ LANGUAGE plpgsql;

-- Popula dim_tempo para 5 anos (2022-2026)
SELECT dw.fn_popular_dim_tempo('2022-01-01'::DATE, '2026-12-31'::DATE) AS dias_inseridos;

-- =============================================================================
-- FATO: dw.fato_vendas (Tabela Fato Principal)
-- =============================================================================
-- A tabela fato contém:
--   1. Chaves estrangeiras para as dimensões (surrogate keys)
--   2. Métricas e medidas (valores numéricos)
--   3. Campos degenerados (informações que não cabem em dimensão)
--
-- GRANULARIDADE: Uma linha = Uma venda (um pedido)
--
-- IMUTABILIDADE: Fatos geralmente não são atualizados, apenas inseridos.
-- Exceção: status_venda pode mudar (PENDENTE → CONCLUIDA/CANCELADA)
-- =============================================================================
DROP TABLE IF EXISTS dw.fato_vendas CASCADE;

CREATE TABLE dw.fato_vendas (
    -- -------------------------------------------------------------------------
    -- Surrogate Keys das Dimensões (FKs)
    -- Note: no Redshift, não usamos FK constraints por performance.
    -- No PostgreSQL local, usamos para validação.
    -- -------------------------------------------------------------------------
    sk_cliente      BIGINT          NOT NULL
                    REFERENCES dw.dim_clientes(sk_cliente),

    sk_produto      BIGINT
                    REFERENCES dw.dim_produto(sk_produto),

    sk_tempo        INTEGER         NOT NULL
                    REFERENCES dw.dim_tempo(sk_tempo),

    -- -------------------------------------------------------------------------
    -- Chaves de Negócio (Degenerate Dimensions - não viram tabelas próprias)
    -- -------------------------------------------------------------------------
    id_venda        BIGINT,                             -- ID original do ERP
    id_pedido       VARCHAR(50),                        -- Número do pedido

    -- -------------------------------------------------------------------------
    -- MÉTRICAS (o coração da tabela fato)
    -- -------------------------------------------------------------------------
    quantidade      INTEGER         NOT NULL DEFAULT 1,
    valor_unitario  DECIMAL(15,2),
    desconto        DECIMAL(15,2)   DEFAULT 0,
    valor_total     DECIMAL(15,2)   NOT NULL,           -- Métrica principal
    custo           DECIMAL(15,2),
    margem_valor    DECIMAL(15,2)   -- Calculado: valor_total - custo
                    GENERATED ALWAYS AS (
                        CASE WHEN custo IS NOT NULL
                             THEN valor_total - custo
                             ELSE NULL
                        END
                    ) STORED,
    margem_pct      DECIMAL(10,4),  -- Percentual de margem

    -- -------------------------------------------------------------------------
    -- Atributos Degenerados (informações da transação, sem dimensão própria)
    -- -------------------------------------------------------------------------
    canal_venda     VARCHAR(30),                        -- ECOMMERCE, LOJA_FISICA, etc.
    status_venda    VARCHAR(20)     DEFAULT 'CONCLUIDA',
    forma_pagamento VARCHAR(30),
    parcelas        INTEGER         DEFAULT 1,
    vendedor_id     VARCHAR(50),
    loja_id         VARCHAR(50),

    -- Timestamps da transação
    data_venda      TIMESTAMP       NOT NULL,
    data_entrega    TIMESTAMP,
    data_cancelamento TIMESTAMP,

    -- -------------------------------------------------------------------------
    -- Auditoria do DW
    -- -------------------------------------------------------------------------
    dw_criado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_batch_id     VARCHAR(50),
    stg_source      VARCHAR(100) DEFAULT 'staging.stg_vendas'

    -- REDSHIFT REAL:
    -- DISTKEY (sk_cliente)  -- Mesmo que dim_clientes para JOINs sem shuffle
    -- SORTKEY (sk_tempo)    -- Queries por período são as mais comuns
    -- ENCODE AUTO
);

COMMENT ON TABLE dw.fato_vendas IS
    'Tabela fato de vendas. Granularidade: 1 linha = 1 venda. '
    'Métricas: valor_total, margem, quantidade.';

-- =============================================================================
-- ÍNDICES para performance analítica
-- (No Redshift: SORTKEY/DISTKEY fazem este papel)
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_fato_vendas_sk_tempo
    ON dw.fato_vendas(sk_tempo);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_sk_cliente
    ON dw.fato_vendas(sk_cliente);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_data_venda
    ON dw.fato_vendas(data_venda);

-- =============================================================================
-- VERIFICAÇÃO FINAL
-- =============================================================================
DO $$
DECLARE
    qtd_dim_tempo INTEGER;
BEGIN
    SELECT COUNT(*) INTO qtd_dim_tempo FROM dw.dim_tempo;

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Tabelas DW criadas com sucesso!';
    RAISE NOTICE '  dim_clientes: criada (SCD Tipo 2)';
    RAISE NOTICE '  dim_produto : criada (SCD Tipo 2)';
    RAISE NOTICE '  dim_tempo   : % dias populados (2022-2026)', qtd_dim_tempo;
    RAISE NOTICE '  fato_vendas : criada';
    RAISE NOTICE '=================================================';
END $$;
