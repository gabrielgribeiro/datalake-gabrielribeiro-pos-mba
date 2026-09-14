"""
ETAPA 4 - AUDITORIA E CONCILIACAO DE INTEGRIDADE

Le de volta os arquivos efetivamente gravados nas tres camadas e comprova a
equivalencia exigida pelo enunciado:

    TOTAL RAW = TOTAL SILVER + TOTAL QUARENTENA

A mesma verificacao e feita no Amazon Athena pela query
sql/05_conciliacao_integridade.sql. Esta etapa em Python serve como
validacao automatizada do pipeline (nao substitui o print do console).
"""

from __future__ import annotations

import json

from src.config import settings
from src.storage import Storage, get_storage


def executar(storage: Storage | None = None) -> dict:
    storage = storage or get_storage()

    pedidos_raw = storage.read_csv(settings.key_raw("pedidos"), dtype={"cliente_id": str})
    fato = storage.read_parquet(settings.key_silver())

    conteudo = storage.read_bytes(settings.key_quarantine()).decode("utf-8")
    rejeitados = [json.loads(linha) for linha in conteudo.splitlines() if linha.strip()]

    total_raw = len(pedidos_raw)
    total_silver = len(fato)
    total_quarentena = len(rejeitados)
    diferenca = total_raw - (total_silver + total_quarentena)
    status = "OK" if diferenca == 0 else "DIVERGENTE"

    print("\n=== ETAPA 4 | AUDITORIA E CONCILIACAO ===")
    print(f"total_raw        : {total_raw}")
    print(f"total_silver     : {total_silver}")
    print(f"total_quarentena : {total_quarentena}")
    print(f"diferenca        : {diferenca}")
    print(f"status           : {status}")

    if status != "OK":
        raise SystemExit(
            "Falha na conciliacao: a soma de Silver + Quarentena nao bate com a camada Raw."
        )

    return {
        "total_raw": total_raw,
        "total_silver": total_silver,
        "total_quarentena": total_quarentena,
        "diferenca": diferenca,
        "status": status,
    }


if __name__ == "__main__":
    executar()
