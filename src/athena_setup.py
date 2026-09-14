"""
ETAPA 5 (APOIO) - PREPARACAO DO AMAZON ATHENA

Duas funcoes:

1) RENDERIZAR  (padrao, nao exige AWS)
   Le os arquivos de sql/, troca o placeholder <SEU-BUCKET> pelo bucket
   configurado e grava versoes prontas para colar no console em sql/generated/.

       python -m src.athena_setup --bucket meu-datalake-mackenzie

2) EXECUTAR (opcional, exige credenciais AWS)
   Executa os DDLs (arquivos 01 a 03) diretamente no Athena via boto3,
   evitando trabalho manual. As queries de auditoria (04 e 05) devem ser
   rodadas no console, porque o enunciado pede o print da tela.

       python -m src.athena_setup --bucket meu-datalake-mackenzie --executar
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

from src.config import PROJECT_ROOT, settings

SQL_DIR = PROJECT_ROOT / "sql"
OUT_DIR = SQL_DIR / "generated"
ARQUIVOS_DDL = ["01_create_database.sql", "02_create_tables_raw.sql",
                "03_create_tables_quarentena_silver_gold.sql"]


def renderizar(bucket: str) -> list[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    gerados = []
    for arquivo in sorted(SQL_DIR.glob("*.sql")):
        conteudo = arquivo.read_text(encoding="utf-8").replace("<SEU-BUCKET>", bucket)
        destino = OUT_DIR / arquivo.name
        destino.write_text(conteudo, encoding="utf-8")
        gerados.append(destino)
        print(f"[athena] gerado: {destino}")
    return gerados


def _statements(sql: str) -> list[str]:
    """Remove comentarios de linha e separa os comandos por ponto e virgula."""
    sem_comentarios = "\n".join(
        linha for linha in sql.splitlines() if not linha.strip().startswith("--")
    )
    return [s.strip() for s in re.split(r";\s*\n", sem_comentarios) if s.strip()]


def executar_ddl(bucket: str) -> None:
    import boto3

    client = boto3.client("athena", region_name=settings.region)
    output = f"s3://{bucket}/{settings.prefix_athena_results}/"

    for nome in ARQUIVOS_DDL:
        sql = (OUT_DIR / nome).read_text(encoding="utf-8")
        for statement in _statements(sql):
            resumo = " ".join(statement.split())[:90]
            resposta = client.start_query_execution(
                QueryString=statement,
                ResultConfiguration={"OutputLocation": output},
                WorkGroup=settings.athena_workgroup,
            )
            execution_id = resposta["QueryExecutionId"]

            while True:
                estado = client.get_query_execution(QueryExecutionId=execution_id)[
                    "QueryExecution"
                ]["Status"]
                if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    break
                time.sleep(1)

            if estado["State"] == "SUCCEEDED":
                print(f"[athena] OK      | {resumo}")
            else:
                motivo = estado.get("StateChangeReason", "sem detalhe")
                print(f"[athena] FALHOU  | {resumo}\n           motivo: {motivo}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepara os scripts e as tabelas do Athena")
    parser.add_argument("--bucket", default=settings.bucket, help="nome do bucket S3")
    parser.add_argument(
        "--executar",
        action="store_true",
        help="executa os DDLs no Athena via boto3 (exige credenciais AWS)",
    )
    args = parser.parse_args()

    renderizar(args.bucket)
    if args.executar:
        executar_ddl(args.bucket)
    else:
        print(
            "\nArquivos prontos em sql/generated/. "
            "Abra o console do Athena e execute na ordem 01 -> 02 -> 03 -> 04 -> 05."
        )


if __name__ == "__main__":
    main()
