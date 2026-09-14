-- =====================================================================
-- 01 | CRIACAO DO BANCO DE DADOS NO AWS GLUE DATA CATALOG (via Athena)
-- ---------------------------------------------------------------------
-- Execute no console do Amazon Athena.
-- Antes de rodar, configure o local de resultado das queries em:
--   Athena > Settings > Query result location
--   s3://<SEU-BUCKET>/athena-results/
-- =====================================================================

CREATE DATABASE IF NOT EXISTS datalake_mackenzie
COMMENT 'Data Lake - Atividade 2 | Medallion Architecture (Raw, Silver, Gold)';
