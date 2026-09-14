"""
Orquestrador do pipeline de Data Lake (Medallion Architecture).

Executa, na ordem:
    1. Ingestao  -> camada Raw     (CSV particionado por ingest_date)
    2. Data Quality + Quarentena + camada Silver (JSON de rejeitados + Parquet/Snappy)
    3. Camada Gold (agregacao analitica por UF e categoria)
    4. Auditoria de conciliacao (Raw = Silver + Quarentena)

Uso:
    python run_pipeline.py                # roda tudo com o destino configurado no .env
    python run_pipeline.py --target s3    # forca gravacao no Amazon S3
    python run_pipeline.py --etapa raw    # roda apenas uma etapa
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline de Data Lake - Raw / Silver / Gold")
    parser.add_argument("--target", choices=["local", "s3"], help="destino da gravacao")
    parser.add_argument("--bucket", help="nome do bucket S3 (sobrescreve o .env)")
    parser.add_argument("--ingest-date", help="data da particao no formato YYYY-MM-DD")
    parser.add_argument(
        "--etapa",
        choices=["raw", "silver", "gold", "auditoria", "tudo"],
        default="tudo",
        help="executa apenas uma etapa especifica",
    )
    parser.add_argument(
        "--criar-bucket",
        action="store_true",
        help="cria o bucket no S3 caso ele ainda nao exista",
    )
    args = parser.parse_args()

    # As variaveis precisam ser definidas ANTES de importar os modulos do pipeline,
    # pois a configuracao e carregada no momento do import.
    if args.target:
        os.environ["DATALAKE_TARGET"] = args.target
    if args.bucket:
        os.environ["S3_BUCKET"] = args.bucket
    if args.ingest_date:
        os.environ["INGEST_DATE"] = args.ingest_date

    from src import audit, build_gold, ingest_raw, process_silver
    from src.config import resumo_configuracao, settings
    from src.storage import S3Storage, get_storage

    inicio = datetime.now()
    print("=" * 78)
    print("PIPELINE DE DATA LAKE - MEDALLION ARCHITECTURE (RAW / SILVER / GOLD)")
    print(resumo_configuracao())
    print("=" * 78)

    storage = get_storage()
    if args.criar_bucket and isinstance(storage, S3Storage):
        storage.ensure_bucket()

    relatorio: dict = {
        "executado_em": inicio.isoformat(timespec="seconds"),
        "destino": settings.target,
        "base_uri": settings.base_uri,
        "ingest_date": settings.ingest_date,
    }

    if args.etapa in ("raw", "tudo"):
        relatorio["etapa_1_raw"] = ingest_raw.executar(storage)
    if args.etapa in ("silver", "tudo"):
        relatorio["etapa_2_silver"] = process_silver.executar(storage)
    if args.etapa in ("gold", "tudo"):
        relatorio["etapa_3_gold"] = build_gold.executar(storage)
    if args.etapa in ("auditoria", "tudo"):
        relatorio["etapa_4_auditoria"] = audit.executar(storage)

    relatorio["duracao_segundos"] = round((datetime.now() - inicio).total_seconds(), 2)

    destino_relatorio = PROJECT_ROOT / "docs" / "relatorio_execucao.json"
    destino_relatorio.parent.mkdir(parents=True, exist_ok=True)
    destino_relatorio.write_text(
        json.dumps(relatorio, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print("\n" + "=" * 78)
    print(f"PIPELINE CONCLUIDO EM {relatorio['duracao_segundos']}s")
    print(f"relatorio de execucao: {destino_relatorio}")
    print("=" * 78)


if __name__ == "__main__":
    main()
