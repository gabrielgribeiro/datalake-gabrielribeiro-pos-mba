# Guia das capturas de tela (Amazon S3 e Amazon Athena)

Este guia mostra, passo a passo, como obter cada print exigido na entrega. Salve todas as imagens na pasta `docs/prints/` com os nomes sugeridos abaixo, o README já aponta para esses arquivos.

---

## Antes de começar

1. Rode o pipeline gravando no S3:

   ```bash
   python run_pipeline.py --target s3 --bucket SEU-BUCKET --criar-bucket
   ```

2. Gere os SQLs já com o nome do seu bucket:

   ```bash
   python -m src.athena_setup --bucket SEU-BUCKET
   ```

3. No console do Athena, configure o local de resultado:
   **Athena → Settings → Manage → Query result location →** `s3://SEU-BUCKET/athena-results/`

4. Execute, no editor de queries, os arquivos `sql/generated/01`, `02` e `03` (nessa ordem). Eles criam o banco, as tabelas externas e descobrem as partições.

> Dica para os prints: deixe visível na tela o nome do banco (`datalake_mackenzie`), a query executada e o resultado. Isso é o que comprova a execução.

---

## Print 1: Estrutura de pastas no S3

**Nome do arquivo:** `docs/prints/01_estrutura_s3.png`

1. Abra o console do **Amazon S3** e entre no seu bucket.
2. Tire um print da raiz mostrando as pastas `raw/`, `quarantine/`, `processed/`, `gold/` e `athena-results/`.
3. Entre em `raw/pedidos/` e tire um segundo print mostrando a pasta `ingest_date=YYYY-MM-DD` (é o que comprova o particionamento Hive).

**Comprova:** ingestão e particionamento correto (critério de 20%).

---

## Print 2: Metadados com `"$path"` e `"$file_size"`

**Nome do arquivo:** `docs/prints/02_athena_metadados_raw.png`

Cole no editor do Athena a query **Q4.1** do arquivo `sql/generated/04_auditoria_metadados.sql`:

```sql
SELECT
    "$path"                                   AS arquivo_origem,
    "$file_size"                              AS tamanho_bytes,
    ROUND("$file_size" / 1024.0, 2)           AS tamanho_kb,
    ingest_date,
    COUNT(*)                                  AS qtd_registros
FROM datalake_mackenzie.raw_pedidos
GROUP BY "$path", "$file_size", ingest_date
ORDER BY ingest_date;
```

**O que esperar:** uma linha por arquivo físico, com a URI completa no S3, o tamanho em bytes e a contagem de registros daquele arquivo.

**Comprova:** uso das pseudo-colunas de metadados e rastreabilidade (parte do critério de 25%).

> As aspas duplas em `"$path"` e `"$file_size"` são obrigatórias. Sem elas o Athena devolve erro de sintaxe.

---

## Print 3: Inventário físico das quatro camadas

**Nome do arquivo:** `docs/prints/03_athena_metadados_camadas.png`

Execute a query **Q4.2** do mesmo arquivo. Ela faz um `UNION ALL` entre Raw, Quarentena, Silver e Gold, mostrando o arquivo e o tamanho de cada camada em uma única tela.

**O que esperar:** quatro blocos de linhas, um por camada, com os caminhos `raw/pedidos/...csv`, `quarantine/...json`, `processed/...parquet` e `gold/...parquet`.

**Opcional (bom para a nota):** execute também a **Q4.3**, que calcula a redução percentual de tamanho do CSV da Raw para o Parquet/Snappy da Silver, e salve como `docs/prints/03b_athena_compressao.png`.

---

## Print 4: Conciliação de integridade (print mais importante)

**Nome do arquivo:** `docs/prints/04_athena_conciliacao.png`

Execute a query **Q5.1** do arquivo `sql/generated/05_conciliacao_integridade.sql`:

```sql
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
```

**O que esperar** (com a configuração padrão do projeto):

| total_raw | total_silver | total_quarentena | soma_silver_quarentena | diferenca | status_conciliacao |
|---|---|---|---|---|---|
| 2000 | 1760 | 240 | 2000 | 0 | OK - INTEGRIDADE CONFIRMADA |

**Comprova:** a equivalência `Raw = Silver + Quarentena` exigida no enunciado.

---

## Print 5: Rejeições por motivo

**Nome do arquivo:** `docs/prints/05_athena_rejeicoes_por_motivo.png`

Execute a query **Q5.2**. Ela agrupa a quarentena por `motivo_rejeicao` e mostra a participação percentual de cada regra violada.

**Opcional (bom para a nota):** execute também a **Q5.3**, que prova que nenhuma anomalia vazou para a Silver (o resultado esperado é zero em todas as colunas), e salve como `docs/prints/05b_athena_silver_limpa.png`.

---

## Print 6: Leitura analítica da camada Gold

**Nome do arquivo:** `docs/prints/06_athena_gold.png`

Execute a query **Q5.5**, que lista as dez melhores combinações de UF e categoria por receita.

**Comprova:** a agregação analítica com métricas de negócio (parte do critério de 30%).

---

## Se algo der errado

| Sintoma | Causa provável | Solução |
|---|---|---|
| `Table not found` | os DDLs não rodaram ou rodaram em outro banco | confirme que o banco `datalake_mackenzie` está selecionado no editor |
| A tabela existe mas devolve zero linhas | as partições não foram descobertas | rode `MSCK REPAIR TABLE datalake_mackenzie.<tabela>;` |
| `Column '$path' cannot be resolved` | faltaram as aspas duplas | use exatamente `"$path"` e `"$file_size"` |
| `No output location provided` | o local de resultado não foi configurado | Athena → Settings → Query result location → `s3://SEU-BUCKET/athena-results/` |
| A quarentena devolve linhas nulas | a partição da quarentena usa `data=`, não `ingest_date=` | confira o `LOCATION` da tabela e rode o `MSCK REPAIR TABLE` |
| `Access Denied` | a role/usuário não tem permissão no bucket | verifique as políticas de S3 e Athena do seu usuário IAM |
