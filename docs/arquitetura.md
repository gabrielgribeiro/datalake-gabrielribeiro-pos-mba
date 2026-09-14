# Decisões de arquitetura

Este documento explica **por que** cada escolha técnica foi feita. Serve tanto como registro do projeto quanto como roteiro de defesa da atividade.

---

## 1. Medallion Architecture (Raw, Silver, Gold)

A arquitetura em camadas separa responsabilidades e evita retrabalho:

| Camada | Papel | Formato | Quem consome |
|---|---|---|---|
| **Raw** | guarda o dado exatamente como chegou, inclusive os erros | CSV | engenharia de dados, auditoria |
| **Quarentena** | isola o que foi reprovado, com o motivo registrado | JSON Lines | time de qualidade, área de negócio |
| **Silver** | dado limpo, validado e enriquecido | Parquet/Snappy | analistas, cientistas de dados |
| **Gold** | dado agregado e pronto para consumo | Parquet/Snappy | dashboards, diretoria |

O ponto central é que a camada Raw **nunca é corrigida**. Se uma regra de negócio mudar, basta reprocessar a Raw e regerar Silver e Gold. É o que torna o pipeline auditável e reprodutível.

---

## 2. Particionamento Hive (`ingest_date=YYYY-MM-DD`)

O padrão `chave=valor` no nome da pasta é reconhecido automaticamente pelo Athena e pelo AWS Glue. Dois ganhos diretos:

- **Custo:** o Athena cobra por volume de dados escaneado. Com partição, uma query filtrando `ingest_date = '2026-09-13'` lê apenas aquela pasta, e não o histórico inteiro.
- **Operação:** cargas de dias diferentes convivem lado a lado sem sobrescrever nada, e um reprocessamento afeta apenas a partição do dia.

A quarentena usa `data=YYYY-MM-DD` porque foi o nome definido no enunciado. A diferença de nome não é problema: cada tabela declara a sua própria coluna de partição.

---

## 3. CSV na Raw, Parquet/Snappy na Silver e na Gold

- **CSV na Raw** porque a camada bruta deve refletir o formato de origem, que na maioria das integrações reais é texto delimitado. CSV também não valida tipos, então as anomalias chegam intactas, que é justamente o que queremos auditar.
- **Parquet/Snappy nas camadas analíticas** porque Parquet é colunar: o Athena lê apenas as colunas usadas na query, reduzindo dados escaneados e, portanto, custo. Snappy é o codec padrão do ecossistema por equilibrar taxa de compressão e velocidade de descompressão, além de ser *splittable* (permite leitura paralela).

A query `Q4.3` do arquivo `sql/04_auditoria_metadados.sql` mede essa redução comparando `"$file_size"` das duas camadas.

---

## 4. Quarentena em JSON Lines

O enunciado pede JSON. Foi usado **JSON Lines** (um objeto JSON por linha, sem vírgula entre eles e sem colchetes envolvendo o arquivo) por um motivo prático: é o formato que o Athena lê nativamente com o `org.openx.data.jsonserde.JsonSerDe`.

Um array JSON tradicional (`[{...}, {...}]`) exigiria pré-processamento antes de ser consultado, o que quebraria a auditoria direto no console.

Cada registro rejeitado carrega:

- todos os campos originais do pedido, sem alteração;
- `motivo_rejeicao`, com o código e a descrição de cada regra violada;
- `qtd_regras_violadas`, para medir a severidade;
- `ingest_date`, para rastrear a carga de origem.

---

## 5. Um registro rejeitado aparece uma única vez

Um mesmo pedido pode violar duas regras ao mesmo tempo (por exemplo, quantidade negativa **e** cliente inexistente). Se ele fosse gravado uma vez por regra, a conciliação `Raw = Silver + Quarentena` deixaria de fechar, porque a quarentena teria mais linhas do que registros reprovados.

A solução foi acumular todos os motivos em um único registro. Isso preserva a identidade `1 registro na Raw = 1 destino`, que é exatamente o que a query de conciliação comprova.

---

## 6. Abstração de armazenamento (local x S3)

O módulo `src/storage.py` expõe a mesma interface para dois destinos: sistema de arquivos local e Amazon S3. O pipeline não sabe onde está gravando, quem decide é a variável `DATALAKE_TARGET`.

Benefícios:

- desenvolver e testar sem gerar custo na AWS;
- rodar os testes automatizados em qualquer máquina, inclusive sem credenciais;
- o código que roda em produção é exatamente o mesmo que foi testado.

---

## 7. Por que Python puro (pandas) e não PySpark

O enunciado aceita Python, PySpark ou AWS Glue. Foi escolhido Python com pandas porque:

- o volume da atividade (milhares de registros) não justifica o overhead de um cluster Spark;
- o projeto roda em qualquer máquina com Python, sem depender de Java, Spark ou de um job do Glue provisionado, o que facilita a reprodução pelo professor;
- a lógica de Data Quality fica explícita e legível, sem ruído de infraestrutura.

Em um cenário de produção com dezenas de milhões de registros por carga, a migração natural seria mover `process_silver.py` e `build_gold.py` para um job PySpark no AWS Glue. A estrutura de pastas, os formatos e as regras de negócio permaneceriam idênticos, mudaria apenas o motor de processamento.

---

## 8. Reprodutibilidade

A geração da massa usa uma semente fixa (`SEED=42`). Rodando o pipeline duas vezes com a mesma configuração, os dados gerados são idênticos, inclusive a quantidade e a distribuição das anomalias. Isso permite que o professor reproduza exatamente os mesmos números apresentados no README.
