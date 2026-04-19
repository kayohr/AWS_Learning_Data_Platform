-- =============================================================================
-- sp_load_staging.sql - Stored Procedure: Carrega dados do S3 para Staging
-- =============================================================================
-- CONCEITO:
--   No Redshift real, a carga do S3 para staging usa o comando COPY:
--     COPY staging.stg_clientes
--     FROM 's3://bucket/processed/clientes/'
--     IAM_ROLE 'arn:aws:iam::123456789:role/redshift-role'
--     FORMAT AS PARQUET;
--
--   Localmente, simula essa carga via INSERT VALUES (dados foram gerados)
--   e chamada do init_db.py que usa psycopg2 para carregar Parquet → PostgreSQL.
-- =============================================================================

CREATE OR REPLACE PROCEDURE staging.sp_load_clientes(
    p_batch_id VARCHAR(50) DEFAULT NULL,
    p_truncar BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id VARCHAR(50);
    v_registros_carregados INTEGER := 0;
    v_inicio TIMESTAMP;
BEGIN
    v_inicio := CURRENT_TIMESTAMP;
    v_batch_id := COALESCE(p_batch_id, 'BATCH_' || TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS'));

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'SP: sp_load_clientes';
    RAISE NOTICE 'Batch ID: %', v_batch_id;
    RAISE NOTICE 'Início: %', v_inicio;

    -- 1. Trunca o staging (full refresh)
    IF p_truncar THEN
        TRUNCATE TABLE staging.stg_clientes;
        RAISE NOTICE 'Tabela stg_clientes truncada para full refresh';
    END IF;

    -- 2. No Redshift REAL, aqui seria o comando COPY:
    -- COPY staging.stg_clientes (cpf, nome, email, ...)
    -- FROM 's3://meu-bucket/processed/clientes/'
    -- IAM_ROLE 'arn:aws:iam::123456789:role/redshift-s3-role'
    -- FORMAT AS PARQUET
    -- ACCEPTINVCHARS '?'
    -- TIMEFORMAT 'auto';

    -- 3. No ambiente LOCAL, o init_db.py faz o COPY via psycopg2.
    --    Esta SP apenas define o contrato e faz validações pós-carga.
    RAISE NOTICE 'Aguardando carga pelo script init_db.py (psycopg2 COPY)...';

    -- 4. Validação pós-carga: verifica se chegaram dados
    SELECT COUNT(*) INTO v_registros_carregados
    FROM staging.stg_clientes
    WHERE stg_batch_id = v_batch_id;

    RAISE NOTICE 'Registros carregados (batch atual): %', v_registros_carregados;

    -- 5. Verifica qualidade mínima: todo cliente deve ter CPF
    PERFORM 1 FROM staging.stg_clientes WHERE cpf IS NULL LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'ERRO QUALIDADE: clientes sem CPF detectados no staging!';
    END IF;

    RAISE NOTICE 'Validação de qualidade: OK (sem CPFs nulos)';
    RAISE NOTICE 'SP sp_load_clientes concluída em %s',
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_inicio))::INTEGER || ' segundos';
    RAISE NOTICE '=================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'ERRO em sp_load_clientes: %', SQLERRM;
        RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE staging.sp_load_vendas(
    p_batch_id VARCHAR(50) DEFAULT NULL,
    p_data_inicio DATE DEFAULT NULL,
    p_data_fim DATE DEFAULT NULL,
    p_truncar BOOLEAN DEFAULT FALSE  -- Vendas normalmente são incremental
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id VARCHAR(50);
    v_registros_carregados INTEGER := 0;
    v_inicio TIMESTAMP;
    v_data_inicio DATE;
    v_data_fim DATE;
BEGIN
    v_inicio := CURRENT_TIMESTAMP;
    v_batch_id := COALESCE(p_batch_id, 'BATCH_' || TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS'));
    v_data_inicio := COALESCE(p_data_inicio, CURRENT_DATE - INTERVAL '1 day');
    v_data_fim := COALESCE(p_data_fim, CURRENT_DATE);

    RAISE NOTICE '=================================================';
    RAISE NOTICE 'SP: sp_load_vendas';
    RAISE NOTICE 'Batch ID: %', v_batch_id;
    RAISE NOTICE 'Período: % a %', v_data_inicio, v_data_fim;

    -- Modo incremental: remove apenas registros do período (evita duplicar)
    IF NOT p_truncar THEN
        DELETE FROM staging.stg_vendas
        WHERE data_venda::DATE BETWEEN v_data_inicio AND v_data_fim;

        RAISE NOTICE 'Registros do período removidos (modo incremental)';
    ELSE
        TRUNCATE TABLE staging.stg_vendas;
        RAISE NOTICE 'Staging de vendas truncado (full refresh)';
    END IF;

    -- Validação pós-carga
    SELECT COUNT(*) INTO v_registros_carregados
    FROM staging.stg_vendas
    WHERE data_venda::DATE BETWEEN v_data_inicio AND v_data_fim;

    RAISE NOTICE 'Registros no período: %', v_registros_carregados;

    -- Verifica valores negativos
    PERFORM 1 FROM staging.stg_vendas WHERE valor_total < 0 LIMIT 1;
    IF FOUND THEN
        RAISE WARNING 'AVISO: Vendas com valor negativo detectadas!';
    END IF;

    RAISE NOTICE 'SP sp_load_vendas concluída!';
    RAISE NOTICE '=================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'ERRO em sp_load_vendas: %', SQLERRM;
        RAISE;
END;
$$;

-- Confirmação
DO $$
BEGIN
    RAISE NOTICE 'Stored Procedures de staging criadas:';
    RAISE NOTICE '  staging.sp_load_clientes()';
    RAISE NOTICE '  staging.sp_load_vendas()';
END $$;
