-- =============================================================================
-- sp_transform_dw.sql - Transforma dados do staging para o DW (Star Schema)
-- =============================================================================

-- =============================================================================
-- SP: Carrega/Atualiza dim_clientes com SCD Tipo 2
-- =============================================================================
CREATE OR REPLACE PROCEDURE dw.sp_transform_dim_clientes(
    p_batch_id VARCHAR(50) DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id VARCHAR(50);
    v_novos INTEGER := 0;
    v_atualizados INTEGER := 0;
BEGIN
    v_batch_id := COALESCE(p_batch_id, 'BATCH_' || TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS'));

    RAISE NOTICE 'SP: sp_transform_dim_clientes - Iniciando SCD Tipo 2';

    -- Passo 1: Fecha registros atuais que mudaram (marca data_fim_vigencia)
    UPDATE dw.dim_clientes d
    SET
        data_fim_vigencia = CURRENT_DATE - 1,
        registro_atual = FALSE,
        dw_atualizado_em = CURRENT_TIMESTAMP
    FROM staging.stg_clientes s
    WHERE d.cpf = s.cpf
      AND d.registro_atual = TRUE
      AND (
          d.nome <> s.nome OR
          d.email IS DISTINCT FROM s.email OR
          d.cidade IS DISTINCT FROM s.cidade OR
          d.uf IS DISTINCT FROM s.uf OR
          d.segmento_cliente IS DISTINCT FROM s.segmento_cliente
      );

    GET DIAGNOSTICS v_atualizados = ROW_COUNT;

    -- Passo 2: Insere novos registros (tanto clientes novos quanto versões novas)
    INSERT INTO dw.dim_clientes (
        cpf, nome, email, telefone, data_nascimento, data_cadastro_crm,
        cidade, uf, cep, regiao, segmento_cliente, limite_credito,
        data_inicio_vigencia, data_fim_vigencia, registro_atual,
        dw_criado_em, dw_atualizado_em
    )
    SELECT DISTINCT ON (s.cpf)
        s.cpf,
        s.nome,
        s.email,
        s.telefone,
        s.data_nascimento,
        s.data_cadastro,
        s.cidade,
        s.uf,
        s.cep,
        dw.fn_uf_para_regiao(s.uf),
        s.segmento_cliente,
        s.limite_credito,
        CURRENT_DATE,
        NULL,
        TRUE,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM staging.stg_clientes s
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_clientes d
        WHERE d.cpf = s.cpf AND d.registro_atual = TRUE
    )
    ORDER BY s.cpf, s.data_cadastro DESC NULLS LAST;

    GET DIAGNOSTICS v_novos = ROW_COUNT;

    RAISE NOTICE 'dim_clientes: % novos, % versões atualizadas (SCD2)', v_novos, v_atualizados;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'ERRO em sp_transform_dim_clientes: %', SQLERRM;
        RAISE;
END;
$$;


-- =============================================================================
-- SP: Carrega dim_produto
-- =============================================================================
CREATE OR REPLACE PROCEDURE dw.sp_transform_dim_produto(
    p_batch_id VARCHAR(50) DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_novos INTEGER := 0;
BEGIN
    RAISE NOTICE 'SP: sp_transform_dim_produto';

    -- Insere produtos novos (ainda não existem na dimensão)
    INSERT INTO dw.dim_produto (
        produto_id, produto_nome, categoria,
        data_inicio_vigencia, registro_atual, dw_criado_em
    )
    SELECT DISTINCT
        s.produto_id,
        s.produto_nome,
        s.categoria_produto,
        CURRENT_DATE,
        TRUE,
        CURRENT_TIMESTAMP
    FROM staging.stg_vendas s
    WHERE s.produto_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM dw.dim_produto p
          WHERE p.produto_id = s.produto_id AND p.registro_atual = TRUE
      );

    GET DIAGNOSTICS v_novos = ROW_COUNT;
    RAISE NOTICE 'dim_produto: % novos produtos inseridos', v_novos;

END;
$$;


-- =============================================================================
-- SP: Carrega fato_vendas
-- =============================================================================
CREATE OR REPLACE PROCEDURE dw.sp_transform_fato_vendas(
    p_batch_id VARCHAR(50) DEFAULT NULL,
    p_data_inicio DATE DEFAULT NULL,
    p_data_fim DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id VARCHAR(50);
    v_registros INTEGER := 0;
    v_sem_cliente INTEGER := 0;
    v_data_inicio DATE;
    v_data_fim DATE;
BEGIN
    v_batch_id := COALESCE(p_batch_id, 'BATCH_' || TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS'));
    v_data_inicio := COALESCE(p_data_inicio, CURRENT_DATE - INTERVAL '1 day');
    v_data_fim := COALESCE(p_data_fim, CURRENT_DATE);

    RAISE NOTICE 'SP: sp_transform_fato_vendas (% a %)', v_data_inicio, v_data_fim;

    -- Remove registros do período para evitar duplicatas (upsert pattern)
    DELETE FROM dw.fato_vendas
    WHERE data_venda::DATE BETWEEN v_data_inicio AND v_data_fim;

    -- Insere vendas com JOINs nas dimensões
    INSERT INTO dw.fato_vendas (
        sk_cliente, sk_produto, sk_tempo,
        id_venda, id_pedido,
        quantidade, valor_unitario, desconto, valor_total, custo, margem_pct,
        canal_venda, status_venda, forma_pagamento, parcelas,
        vendedor_id, loja_id,
        data_venda, data_entrega, data_cancelamento,
        dw_criado_em, dw_batch_id
    )
    SELECT
        dc.sk_cliente,
        dp.sk_produto,
        dt.sk_tempo,
        sv.id_venda,
        sv.id_pedido,
        COALESCE(sv.quantidade, 1),
        sv.valor_unitario,
        COALESCE(sv.desconto, 0),
        sv.valor_total,
        sv.custo,
        sv.margem_pct,
        sv.canal_venda,
        COALESCE(sv.status_venda, 'CONCLUIDA'),
        sv.forma_pagamento,
        COALESCE(sv.parcelas, 1),
        sv.vendedor_id,
        sv.loja_id,
        sv.data_venda,
        sv.data_entrega,
        sv.data_cancelamento,
        CURRENT_TIMESTAMP,
        v_batch_id
    FROM staging.stg_vendas sv
    -- JOIN com dimensão cliente (pega a versão vigente)
    INNER JOIN dw.dim_clientes dc
        ON dc.cpf = sv.cpf_cliente
        AND dc.registro_atual = TRUE
    -- JOIN com dimensão tempo
    INNER JOIN dw.dim_tempo dt
        ON dt.sk_tempo = TO_CHAR(sv.data_venda, 'YYYYMMDD')::INTEGER
    -- JOIN com dimensão produto (LEFT: vendas sem produto também entram)
    LEFT JOIN dw.dim_produto dp
        ON dp.produto_id = sv.produto_id
        AND dp.registro_atual = TRUE
    WHERE sv.data_venda::DATE BETWEEN v_data_inicio AND v_data_fim
      AND sv.valor_total > 0;

    GET DIAGNOSTICS v_registros = ROW_COUNT;

    -- Conta vendas sem cliente correspondente (para monitoramento)
    SELECT COUNT(*) INTO v_sem_cliente
    FROM staging.stg_vendas sv
    WHERE sv.data_venda::DATE BETWEEN v_data_inicio AND v_data_fim
      AND NOT EXISTS (
          SELECT 1 FROM dw.dim_clientes dc
          WHERE dc.cpf = sv.cpf_cliente AND dc.registro_atual = TRUE
      );

    RAISE NOTICE 'fato_vendas: % registros inseridos', v_registros;
    IF v_sem_cliente > 0 THEN
        RAISE WARNING 'AVISO: % vendas sem cliente correspondente (CPF não encontrado)', v_sem_cliente;
    END IF;

    -- No Redshift real: depois de grandes cargas, executaria:
    -- VACUUM SORT ONLY dw.fato_vendas;
    -- ANALYZE dw.fato_vendas;
    RAISE NOTICE 'DICA REDSHIFT: Execute VACUUM e ANALYZE após grandes cargas';

END;
$$;


-- =============================================================================
-- SP: Atualiza todos os Data Marts
-- =============================================================================
CREATE OR REPLACE PROCEDURE dm.sp_refresh_data_marts(
    p_batch_id VARCHAR(50) DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id VARCHAR(50);
BEGIN
    v_batch_id := COALESCE(p_batch_id, 'BATCH_' || TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS'));
    RAISE NOTICE 'SP: sp_refresh_data_marts - batch %', v_batch_id;

    -- Refresh dm_vendas_diarias
    TRUNCATE TABLE dm.dm_vendas_diarias;

    INSERT INTO dm.dm_vendas_diarias (
        data_venda, ano, mes, mes_nome, trimestre, dia_semana, fim_de_semana,
        uf, regiao,
        qtd_vendas, qtd_clientes_unicos,
        receita_total, ticket_medio, desconto_total, margem_total, margem_media_pct,
        dm_atualizado_em, dm_batch_id
    )
    SELECT
        fv.data_venda::DATE                 AS data_venda,
        dt.ano,
        dt.mes,
        dt.mes_nome,
        dt.trimestre,
        dt.dia_semana_nome                  AS dia_semana,
        dt.fim_de_semana,
        dc.uf,
        dc.regiao,
        COUNT(*)                            AS qtd_vendas,
        COUNT(DISTINCT dc.cpf)              AS qtd_clientes_unicos,
        SUM(fv.valor_total)                 AS receita_total,
        AVG(fv.valor_total)                 AS ticket_medio,
        SUM(COALESCE(fv.desconto, 0))       AS desconto_total,
        SUM(COALESCE(fv.valor_total - fv.custo, 0)) AS margem_total,
        AVG(COALESCE(fv.margem_pct, 0))     AS margem_media_pct,
        CURRENT_TIMESTAMP,
        v_batch_id
    FROM dw.fato_vendas fv
    JOIN dw.dim_tempo dt     ON dt.sk_tempo = fv.sk_tempo
    JOIN dw.dim_clientes dc  ON dc.sk_cliente = fv.sk_cliente
    WHERE fv.status_venda != 'CANCELADA'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

    RAISE NOTICE 'dm_vendas_diarias: atualizado';

    -- Refresh dm_top_clientes (RFM)
    TRUNCATE TABLE dm.dm_top_clientes;

    INSERT INTO dm.dm_top_clientes (
        cpf, nome, email, uf, segmento_cliente,
        total_pedidos, receita_total, ticket_medio,
        ultima_compra, primeira_compra,
        dias_desde_ultima, dias_como_cliente,
        ranking_receita, dm_atualizado_em
    )
    SELECT
        dc.cpf,
        dc.nome,
        dc.email,
        dc.uf,
        dc.segmento_cliente,
        COUNT(fv.id_venda)              AS total_pedidos,
        SUM(fv.valor_total)             AS receita_total,
        AVG(fv.valor_total)             AS ticket_medio,
        MAX(fv.data_venda::DATE)        AS ultima_compra,
        MIN(fv.data_venda::DATE)        AS primeira_compra,
        (CURRENT_DATE - MAX(fv.data_venda::DATE)) AS dias_desde_ultima,
        (CURRENT_DATE - MIN(fv.data_venda::DATE)) AS dias_como_cliente,
        ROW_NUMBER() OVER (ORDER BY SUM(fv.valor_total) DESC)::INTEGER AS ranking_receita,
        CURRENT_TIMESTAMP
    FROM dw.fato_vendas fv
    JOIN dw.dim_clientes dc ON dc.sk_cliente = fv.sk_cliente AND dc.registro_atual = TRUE
    WHERE fv.status_venda != 'CANCELADA'
    GROUP BY dc.cpf, dc.nome, dc.email, dc.uf, dc.segmento_cliente;

    RAISE NOTICE 'dm_top_clientes: atualizado';
    RAISE NOTICE 'Todos os Data Marts atualizados com sucesso!';

END;
$$;

-- Confirmação
DO $$
BEGIN
    RAISE NOTICE 'Stored Procedures de transformação criadas:';
    RAISE NOTICE '  dw.sp_transform_dim_clientes() - SCD Tipo 2';
    RAISE NOTICE '  dw.sp_transform_dim_produto()';
    RAISE NOTICE '  dw.sp_transform_fato_vendas()';
    RAISE NOTICE '  dm.sp_refresh_data_marts()';
END $$;
