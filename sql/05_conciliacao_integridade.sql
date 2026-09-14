-- =====================================================================
-- 05 | CONCILIACAO DE INTEGRIDADE DOS DADOS
-- ---------------------------------------------------------------------
-- Comprova a equivalencia exigida no enunciado:
--
--        TOTAL RAW  =  TOTAL SILVER  +  TOTAL QUARENTENA
--
-- Ou seja: nenhum registro foi perdido silenciosamente pelo pipeline.
-- Todo pedido que entrou na camada Raw ou virou fato valido (Silver)
-- ou foi isolado na quarentena com o motivo da rejeicao.
-- =====================================================================

-- Q5.1 | CONCILIACAO PRINCIPAL (esta e a query do print obrigatorio)
WITH total_raw AS (
    SELECT COUNT(*) AS qtd FROM datalake_mackenzie.raw_pedidos
),
total_silver AS (
    SELECT COUNT(*) AS qtd FROM datalake_mackenzie.fato_vendas
),
total_quarentena AS (
    SELECT COUNT(*) AS qtd FROM datalake_mackenzie.quarentena_pedidos_rejeitados
)
SELECT
    r.qtd                                   AS total_raw,
    s.qtd                                   AS total_silver,
    q.qtd                                   AS total_quarentena,
    (s.qtd + q.qtd)                         AS soma_silver_quarentena,
    (r.qtd - (s.qtd + q.qtd))               AS diferenca,
    CASE WHEN r.qtd = s.qtd + q.qtd
         THEN 'OK - INTEGRIDADE CONFIRMADA'
         ELSE 'DIVERGENCIA - INVESTIGAR'
    END                                     AS status_conciliacao
FROM total_raw r, total_silver s, total_quarentena q;


-- Q5.2 | Distribuicao dos registros rejeitados por motivo
SELECT
    motivo_rejeicao,
    COUNT(*)                                                        AS qtd_registros,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)              AS percentual
FROM datalake_mackenzie.quarentena_pedidos_rejeitados
GROUP BY motivo_rejeicao
ORDER BY qtd_registros DESC;


-- Q5.3 | Prova de que nenhuma anomalia vazou para a camada Silver
-- O resultado esperado e ZERO em todas as colunas.
SELECT
    SUM(CASE WHEN quantidade <= 0 THEN 1 ELSE 0 END)                        AS qtd_invalida_na_silver,
    SUM(CASE WHEN cliente_id  NOT IN (SELECT cliente_id FROM datalake_mackenzie.raw_clientes) THEN 1 ELSE 0 END) AS cliente_orfao_na_silver,
    SUM(CASE WHEN product_id  NOT IN (SELECT product_id FROM datalake_mackenzie.raw_produtos) THEN 1 ELSE 0 END) AS produto_orfao_na_silver
FROM datalake_mackenzie.fato_vendas;


-- Q5.4 | Conciliacao financeira: Silver x Gold devem bater centavo a centavo
SELECT
    (SELECT ROUND(SUM(valor_total), 2)   FROM datalake_mackenzie.fato_vendas)             AS receita_silver,
    (SELECT ROUND(SUM(receita_total), 2) FROM datalake_mackenzie.agg_vendas_uf_categoria) AS receita_gold,
    (SELECT ROUND(SUM(valor_total), 2)   FROM datalake_mackenzie.fato_vendas)
  - (SELECT ROUND(SUM(receita_total), 2) FROM datalake_mackenzie.agg_vendas_uf_categoria) AS diferenca;


-- Q5.5 | Leitura analitica da camada Gold (top 10 UF x categoria por receita)
SELECT
    uf,
    categoria,
    qtd_pedidos,
    qtd_itens,
    receita_total,
    ticket_medio,
    clientes_distintos
FROM datalake_mackenzie.agg_vendas_uf_categoria
ORDER BY receita_total DESC
LIMIT 10;
