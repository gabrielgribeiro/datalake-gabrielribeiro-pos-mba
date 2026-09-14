-- =====================================================================
-- 02 | TABELAS EXTERNAS DA CAMADA RAW (CSV particionado - Hive Style)
-- ---------------------------------------------------------------------
-- Substitua <SEU-BUCKET> pelo nome do seu bucket antes de executar.
-- As tabelas apontam para o diretorio PAI da particao; o Athena
-- descobre as particoes ingest_date=YYYY-MM-DD com MSCK REPAIR TABLE.
-- =====================================================================

-- ------------------------------------------------- dimensao: clientes
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_mackenzie.raw_clientes (
    cliente_id      string,
    nome_cliente    string,
    uf              string,
    cidade          string,
    segmento        string,
    data_cadastro   string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://<SEU-BUCKET>/raw/clientes/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- ------------------------------------------------- dimensao: produtos
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_mackenzie.raw_produtos (
    product_id      string,
    nome_produto    string,
    categoria       string,
    preco           double,
    fornecedor      string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://<SEU-BUCKET>/raw/produtos/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- ------------------------------------------------------- fato: pedidos
-- Observacao: a camada Raw guarda o dado como ele chegou, inclusive as
-- anomalias (quantidade <= 0 e chaves inexistentes). Nenhum filtro aqui.
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_mackenzie.raw_pedidos (
    pedido_id       string,
    cliente_id      string,
    product_id      string,
    quantidade      int,
    data_pedido     string,
    canal_venda     string
)
PARTITIONED BY (ingest_date string)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://<SEU-BUCKET>/raw/pedidos/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- ----------------------------------------- descoberta das particoes
MSCK REPAIR TABLE datalake_mackenzie.raw_clientes;
MSCK REPAIR TABLE datalake_mackenzie.raw_produtos;
MSCK REPAIR TABLE datalake_mackenzie.raw_pedidos;
