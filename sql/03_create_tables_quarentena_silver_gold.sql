-- =====================================================================
-- 03 | TABELAS EXTERNAS DA QUARENTENA, DA CAMADA SILVER E DA CAMADA GOLD
-- ---------------------------------------------------------------------
-- Substitua <SEU-BUCKET> pelo nome do seu bucket antes de executar.
-- =====================================================================

-- ---------------------------------------------------------- QUARENTENA
-- Arquivo em JSON Lines (um objeto JSON por linha), formato lido
-- nativamente pelo Athena com o JSON SerDe.
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_mackenzie.quarentena_pedidos_rejeitados (
    pedido_id               string,
    cliente_id              string,
    product_id              string,
    quantidade              int,
    data_pedido             string,
    canal_venda             string,
    motivo_rejeicao         string,
    qtd_regras_violadas     int,
    ingest_date             string
)
PARTITIONED BY (data string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://<SEU-BUCKET>/quarantine/pedidos_rejeitados/';

-- ------------------------------------------------- SILVER: fato_vendas
-- Parquet com compressao Snappy, ja enriquecido e com valor_total.
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_mackenzie.fato_vendas (
    pedido_id       string,
    data_pedido     string,
    canal_venda     string,
    cliente_id      string,
    nome_cliente    string,
    uf              string,
    cidade          string,
    segmento        string,
    product_id      string,
    nome_produto    string,
    categoria       string,
    fornecedor      string,
    preco           double,
    quantidade      bigint,
    valor_total     double
)
PARTITIONED BY (ingest_date string)
STORED AS PARQUET
LOCATION 's3://<SEU-BUCKET>/processed/fato_vendas/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ------------------------------ GOLD: agregacao por UF e categoria
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_mackenzie.agg_vendas_uf_categoria (
    uf                  string,
    categoria           string,
    qtd_pedidos         bigint,
    qtd_itens           bigint,
    receita_total       double,
    ticket_medio        double,
    preco_medio_item    double,
    clientes_distintos  bigint,
    produtos_distintos  bigint
)
PARTITIONED BY (ingest_date string)
STORED AS PARQUET
LOCATION 's3://<SEU-BUCKET>/gold/agg_vendas_uf_categoria/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ----------------------------------------- descoberta das particoes
MSCK REPAIR TABLE datalake_mackenzie.quarentena_pedidos_rejeitados;
MSCK REPAIR TABLE datalake_mackenzie.fato_vendas;
MSCK REPAIR TABLE datalake_mackenzie.agg_vendas_uf_categoria;
