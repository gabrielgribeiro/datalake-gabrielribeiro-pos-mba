# Capturas de tela

Coloque aqui as imagens do console da AWS, seguindo os nomes abaixo. O passo a passo de cada uma está em [`../guia_prints_athena.md`](../guia_prints_athena.md).

| Arquivo | Conteúdo |
|---|---|
| `01_estrutura_s3.png` | pastas `raw/`, `quarantine/`, `processed/`, `gold/` e `athena-results/` no console do S3, mostrando a partição `ingest_date=YYYY-MM-DD` |
| `02_athena_metadados_raw.png` | query Q4.1, com as pseudo-colunas `"$path"` e `"$file_size"` |
| `03_athena_metadados_camadas.png` | query Q4.2, inventário físico das quatro camadas |
| `03b_athena_compressao.png` | *(opcional)* query Q4.3, ganho de compressão CSV x Parquet |
| `04_athena_conciliacao.png` | query Q5.1, conciliação `Raw = Silver + Quarentena` |
| `05_athena_rejeicoes_por_motivo.png` | query Q5.2, distribuição das rejeições por regra |
| `05b_athena_silver_limpa.png` | *(opcional)* query Q5.3, prova de que nenhuma anomalia vazou para a Silver |
| `06_athena_gold.png` | query Q5.5, leitura analítica da camada Gold |
