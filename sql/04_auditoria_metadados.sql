-- =====================================================================
-- 04 | AUDITORIA DE METADADOS COM AS PSEUDO-COLUNAS "$path" E "$file_size"
-- ---------------------------------------------------------------------
-- O Athena expoe pseudo-colunas de metadados para tabelas externas:
--   "$path"      -> URI completa do objeto no S3 que originou a linha
--   "$file_size" -> tamanho do objeto em bytes
-- Elas comprovam a rastreabilidade (lineage) de cada registro ate o
-- arquivo fisico no data lake.
-- ATENCAO: as aspas duplas sao obrigatorias.
-- =====================================================================

-- Q4.1 | Inventario fisico da camada RAW (arquivo, tamanho e volumetria)
SELECT
    "$path"                                   AS arquivo_origem,
    "$file_size"                              AS tamanho_bytes,
    ROUND("$file_size" / 1024.0, 2)           AS tamanho_kb,
    ingest_date,
    COUNT(*)                                  AS qtd_registros
FROM datalake_mackenzie.raw_pedidos
GROUP BY "$path", "$file_size", ingest_date
ORDER BY ingest_date;


-- Q4.2 | Inventario fisico das tres camadas em uma unica visao
SELECT 'RAW / pedidos'      AS camada, "$path" AS arquivo_origem, "$file_size" AS tamanho_bytes, COUNT(*) AS qtd_registros
FROM datalake_mackenzie.raw_pedidos                   GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'QUARENTENA'         AS camada, "$path", "$file_size", COUNT(*)
FROM datalake_mackenzie.quarentena_pedidos_rejeitados GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'SILVER / fato'      AS camada, "$path", "$file_size", COUNT(*)
FROM datalake_mackenzie.fato_vendas                   GROUP BY "$path", "$file_size"
UNION ALL
SELECT 'GOLD / agregado'    AS camada, "$path", "$file_size", COUNT(*)
FROM datalake_mackenzie.agg_vendas_uf_categoria       GROUP BY "$path", "$file_size"
ORDER BY camada;


-- Q4.3 | Comprovacao do ganho de compressao Raw (CSV) x Silver (Parquet/Snappy)
WITH raw_fisico AS (
    SELECT DISTINCT "$path" AS arquivo, "$file_size" AS bytes
    FROM datalake_mackenzie.raw_pedidos
),
silver_fisico AS (
    SELECT DISTINCT "$path" AS arquivo, "$file_size" AS bytes
    FROM datalake_mackenzie.fato_vendas
)
SELECT
    (SELECT SUM(bytes) FROM raw_fisico)                                              AS bytes_raw_csv,
    (SELECT SUM(bytes) FROM silver_fisico)                                           AS bytes_silver_parquet,
    ROUND(100.0 * (1 - CAST((SELECT SUM(bytes) FROM silver_fisico) AS double)
                     / (SELECT SUM(bytes) FROM raw_fisico)), 2)                      AS reducao_percentual;


-- Q4.4 | Amostra de linhas com o arquivo de origem (lineage registro a registro)
SELECT
    pedido_id,
    cliente_id,
    product_id,
    quantidade,
    valor_total,
    "$path" AS arquivo_origem
FROM datalake_mackenzie.fato_vendas
LIMIT 10;
