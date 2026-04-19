-- =============================================================================
-- 001_create_schemas.sql - Criação dos Schemas do Data Warehouse
-- =============================================================================
-- CONCEITO AWS REDSHIFT:
--   No Redshift real, schemas são como "namespaces" ou "pastas" dentro do banco.
--   Permitem organizar tabelas por camada de dados e controlar acesso via IAM.
--
--   Boas práticas de schemas:
--     staging → dados brutos vindos do S3 (temporário)
--     dw      → Data Warehouse dimensional (star schema)
--     dm      → Data Marts (agregações pré-calculadas para BI)
--
-- EQUIVALENTE POSTGRESQL:
--   Este SQL funciona igual no PostgreSQL local e no Redshift real.
--   No Redshift, você adicionaria também:
--     - GRANT para grupos IAM
--     - Configurações de WLM (Workload Management)
-- =============================================================================

-- Remove schemas se já existirem (para recriar limpo)
-- CUIDADO em produção: isso deleta TODOS os dados!
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS dw CASCADE;
DROP SCHEMA IF EXISTS dm CASCADE;

-- =============================================================================
-- SCHEMA: staging
-- =============================================================================
-- CONCEITO:
--   Zona de aterrissagem dos dados vindos do S3.
--   Os dados chegam aqui exatamente como estão no S3 processed/.
--   É uma área temporária - dados são processados e movidos para o dw.
--
-- No Redshift real:
--   COPY staging.clientes
--   FROM 's3://bucket/processed/clientes/'
--   IAM_ROLE 'arn:aws:iam::123456789:role/redshift-s3-role'
--   FORMAT AS PARQUET;
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS staging
    COMMENT 'Schema de staging: dados brutos vindos do S3, zona temporária';

-- No Redshift real: concederia acesso ao role do Glue/COPY
-- GRANT USAGE ON SCHEMA staging TO glue_role;
-- GRANT CREATE ON SCHEMA staging TO glue_role;

-- =============================================================================
-- SCHEMA: dw (Data Warehouse)
-- =============================================================================
-- CONCEITO:
--   Schema principal do Data Warehouse.
--   Contém o Star Schema: tabelas de dimensão (dim_*) e fatos (fato_*).
--   Dados aqui são históricos e imutáveis (append-only para fatos).
--
-- Star Schema (Esquema Estrela):
--   - Tabelas DIMENSÃO: contêm atributos descritivos (quem, o quê, quando, onde)
--   - Tabelas FATO: contêm métricas e medidas (valores, quantidades)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS dw
    COMMENT 'Schema do Data Warehouse: Star Schema com dimensões e fatos';

-- =============================================================================
-- SCHEMA: dm (Data Marts)
-- =============================================================================
-- CONCEITO:
--   Data Marts são subconjuntos do DW focados em uma área de negócio.
--   Contêm dados pré-agregados para dashboards e relatórios.
--   Views materializadas ou tabelas agregadas que o BI (Power BI, Tableau) consulta.
--
-- Vantagens:
--   1. Queries mais rápidas (dados já agregados)
--   2. Menor custo (Redshift cobra por dados escaneados no Redshift Spectrum)
--   3. Proteção: analistas de negócio não acessam dados brutos
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS dm
    COMMENT 'Schema de Data Marts: agregações pré-calculadas para BI e relatórios';

-- =============================================================================
-- VERIFICAÇÃO
-- =============================================================================
-- Lista os schemas criados
SELECT schema_name,
       schema_owner,
       -- No Redshift real: teria mais colunas como schema_type
       'schema criado com sucesso' AS status
FROM information_schema.schemata
WHERE schema_name IN ('staging', 'dw', 'dm')
ORDER BY schema_name;

-- Mensagem de confirmação
DO $$
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Schemas criados com sucesso:';
    RAISE NOTICE '  - staging: zona de aterrissagem S3 → Redshift';
    RAISE NOTICE '  - dw: Data Warehouse (Star Schema)';
    RAISE NOTICE '  - dm: Data Marts (agregações para BI)';
    RAISE NOTICE '==============================================';
END $$;
