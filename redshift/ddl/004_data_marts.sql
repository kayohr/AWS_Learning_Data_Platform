-- =============================================================================
-- 004_data_marts.sql - Data Marts (Views e Tabelas Agregadas para BI)
-- =============================================================================
-- CONCEITO DATA MARTS:
--   Data Marts são subconjuntos do DW com dados pré-agregados por área de negócio.
--   Analistas de BI consultam os data marts, não as tabelas fato diretamente.
--
--   Benefícios:
--     1. PERFORMANCE: Agregações já calculadas = queries mais rápidas
--     2. CUSTO: No Redshift, menos dados escaneados = menor custo
--        (Redshift Spectrum cobra $5/TB escaneado)
--     3. SEGURANÇA: Analistas acessam apenas dm.*, não dw.fato_vendas
--     4. SIMPLICIDADE: Métricas de negócio já calculadas
--
-- TIPO DE IMPLEMENTAÇÃO:
--   Views: calculadas em tempo real (sempre atualizadas, mais lentas)
--   Materialized Views: calculadas e armazenadas (rápidas, precisam refresh)
--   Tabelas: calculadas por stored procedure, mais controle
-- =============================================================================

-- =============================================================================
-- DATA MART 1: Vendas por Dia e Estado
-- =============================================================================
DROP TABLE IF EXISTS dm.dm_vendas_diarias CASCADE;

CREATE TABLE dm.dm_vendas_diarias (
    -- Chaves de análise
    data_venda          DATE            NOT NULL,
    ano                 SMALLINT        NOT NULL,
    mes                 SMALLINT        NOT NULL,
    mes_nome            VARCHAR(20),
    trimestre           SMALLINT,
    dia_semana          VARCHAR(20),
    fim_de_semana       BOOLEAN,
    uf                  CHAR(2),
    regiao              VARCHAR(20),

    -- Métricas de volume
    qtd_vendas          INTEGER         DEFAULT 0,
    qtd_vendas_ecommerce INTEGER        DEFAULT 0,
    qtd_vendas_loja     INTEGER         DEFAULT 0,
    qtd_clientes_unicos INTEGER         DEFAULT 0,

    -- Métricas financeiras (em R$)
    receita_total       DECIMAL(18,2)   DEFAULT 0,
    ticket_medio        DECIMAL(15,2)   DEFAULT 0,
    receita_ecommerce   DECIMAL(18,2)   DEFAULT 0,
    receita_loja        DECIMAL(18,2)   DEFAULT 0,
    desconto_total      DECIMAL(18,2)   DEFAULT 0,
    margem_total        DECIMAL(18,2)   DEFAULT 0,
    margem_media_pct    DECIMAL(10,4)   DEFAULT 0,

    -- Métricas de crescimento (calculadas em relação ao dia anterior)
    receita_dia_anterior DECIMAL(18,2),
    crescimento_pct     DECIMAL(10,4),  -- (receita / receita_anterior - 1) * 100

    -- Controle do DM
    dm_atualizado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dm_batch_id         VARCHAR(50),

    -- Chave primária composta
    PRIMARY KEY (data_venda, uf)
);

COMMENT ON TABLE dm.dm_vendas_diarias IS
    'Data Mart: vendas agregadas por dia e estado (UF). '
    'Atualizado diariamente pelo pipeline. Base para dashboards de vendas.';

-- =============================================================================
-- DATA MART 2: Performance por Canal de Vendas
-- =============================================================================
DROP TABLE IF EXISTS dm.dm_vendas_canal CASCADE;

CREATE TABLE dm.dm_vendas_canal (
    ano             SMALLINT        NOT NULL,
    mes             SMALLINT        NOT NULL,
    canal_venda     VARCHAR(30)     NOT NULL,
    forma_pagamento VARCHAR(30),

    qtd_vendas          INTEGER         DEFAULT 0,
    qtd_clientes_unicos INTEGER         DEFAULT 0,
    receita_total       DECIMAL(18,2)   DEFAULT 0,
    ticket_medio        DECIMAL(15,2)   DEFAULT 0,
    desconto_total      DECIMAL(18,2)   DEFAULT 0,
    margem_media_pct    DECIMAL(10,4)   DEFAULT 0,

    -- Participação no total (share %)
    share_receita_pct   DECIMAL(10,4)   DEFAULT 0,

    dm_atualizado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ano, mes, canal_venda, forma_pagamento)
);

COMMENT ON TABLE dm.dm_vendas_canal IS
    'Performance de vendas por canal (ECOMMERCE, LOJA_FISICA, etc.) e mês.';

-- =============================================================================
-- DATA MART 3: Ranking de Clientes (RFM Simplificado)
-- =============================================================================
-- RFM = Recência, Frequência, Monetário
-- Técnica de segmentação de clientes muito usada em marketing
-- =============================================================================
DROP TABLE IF EXISTS dm.dm_top_clientes CASCADE;

CREATE TABLE dm.dm_top_clientes (
    cpf             VARCHAR(11)     NOT NULL PRIMARY KEY,
    nome            VARCHAR(200),
    email           VARCHAR(200),
    uf              CHAR(2),
    segmento_cliente VARCHAR(10),

    -- Métricas RFM
    total_pedidos       INTEGER         DEFAULT 0,      -- Frequência
    receita_total       DECIMAL(18,2)   DEFAULT 0,      -- Monetário
    ticket_medio        DECIMAL(15,2)   DEFAULT 0,
    ultima_compra       DATE,                           -- Recência
    primeira_compra     DATE,
    dias_desde_ultima   INTEGER,                        -- Calculado: hoje - ultima_compra
    dias_como_cliente   INTEGER,                        -- Calculado: hoje - primeira_compra

    -- Score RFM (1-5, onde 5 é melhor)
    score_recencia      SMALLINT,   -- 5=comprou recentemente, 1=faz tempo
    score_frequencia    SMALLINT,   -- 5=compra muito, 1=poucas compras
    score_monetario     SMALLINT,   -- 5=gasta muito, 1=pouco gasto
    segmento_rfm        VARCHAR(50),-- "Campeão", "Cliente Fiel", "Em Risco", etc.

    -- Classificação geral
    ranking_receita     INTEGER,    -- 1=maior comprador

    dm_atualizado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dm.dm_top_clientes IS
    'Análise RFM de clientes: Recência, Frequência e Monetário. '
    'Base para campanhas de marketing segmentadas.';

-- =============================================================================
-- VIEWS PARA ANÁLISE RÁPIDA
-- (No Redshift: considere Materialized Views para melhor performance)
-- =============================================================================

-- View: KPIs do mês atual
CREATE OR REPLACE VIEW dm.vw_kpis_mes_atual AS
SELECT
    EXTRACT(YEAR FROM CURRENT_DATE)::SMALLINT   AS ano,
    EXTRACT(MONTH FROM CURRENT_DATE)::SMALLINT  AS mes,
    SUM(receita_total)                          AS receita_mes,
    SUM(qtd_vendas)                             AS pedidos_mes,
    AVG(ticket_medio)                           AS ticket_medio_mes,
    AVG(margem_media_pct)                       AS margem_media_mes,
    COUNT(DISTINCT uf)                          AS estados_atendidos
FROM dm.dm_vendas_diarias
WHERE ano = EXTRACT(YEAR FROM CURRENT_DATE)
  AND mes = EXTRACT(MONTH FROM CURRENT_DATE);

COMMENT ON VIEW dm.vw_kpis_mes_atual IS
    'KPIs consolidados do mês atual. Consulta rápida para dashboard executivo.';

-- View: Comparativo YoY (Year over Year - comparação ano a ano)
CREATE OR REPLACE VIEW dm.vw_receita_yoy AS
SELECT
    ano,
    mes,
    mes_nome,
    SUM(receita_total)                          AS receita_total,
    LAG(SUM(receita_total)) OVER (
        PARTITION BY mes ORDER BY ano
    )                                           AS receita_ano_anterior,
    ROUND(
        (SUM(receita_total) / NULLIF(
            LAG(SUM(receita_total)) OVER (
                PARTITION BY mes ORDER BY ano
            ), 0
        ) - 1) * 100, 2
    )                                           AS crescimento_yoy_pct
FROM dm.dm_vendas_diarias
GROUP BY ano, mes, mes_nome
ORDER BY ano, mes;

COMMENT ON VIEW dm.vw_receita_yoy IS
    'Comparativo de receita ano a ano (YoY). '
    'Crescimento positivo = melhor que mesmo período do ano anterior.';

-- View: Funil de Canal de Vendas
CREATE OR REPLACE VIEW dm.vw_funil_canal AS
SELECT
    canal_venda,
    SUM(qtd_vendas)         AS total_pedidos,
    SUM(receita_total)      AS receita_total,
    AVG(ticket_medio)       AS ticket_medio,
    AVG(margem_media_pct)   AS margem_media_pct,
    ROUND(
        SUM(receita_total) * 100.0 / NULLIF(
            SUM(SUM(receita_total)) OVER (), 0
        ), 2
    )                       AS share_receita_pct
FROM dm.dm_vendas_canal
GROUP BY canal_venda
ORDER BY receita_total DESC;

COMMENT ON VIEW dm.vw_funil_canal IS 'Participação de cada canal no total de vendas.';

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Data Marts criados com sucesso!';
    RAISE NOTICE '  dm.dm_vendas_diarias: Por dia e estado';
    RAISE NOTICE '  dm.dm_vendas_canal  : Por canal de venda';
    RAISE NOTICE '  dm.dm_top_clientes  : Análise RFM';
    RAISE NOTICE '';
    RAISE NOTICE 'Views criadas:';
    RAISE NOTICE '  dm.vw_kpis_mes_atual: KPIs do mês corrente';
    RAISE NOTICE '  dm.vw_receita_yoy   : Comparativo Year over Year';
    RAISE NOTICE '  dm.vw_funil_canal   : Share por canal de vendas';
    RAISE NOTICE '=================================================';
END $$;
