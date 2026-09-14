"""
ETAPA 2 - DATA QUALITY, QUARENTENA E CAMADA SILVER

Le os dados brutos da camada Raw e aplica as regras de governanca:

Regras de qualidade (Data Quality)
    R1 - quantidade deve ser numerica
    R2 - descartar registros com quantidade <= 0
    R3 - descartar registros cujo cliente_id nao exista na dimensao clientes
    R4 - descartar registros cujo product_id nao exista na dimensao produtos

Quarentena
    Todos os registros invalidos sao gravados com o motivo da rejeicao em JSON:
        quarantine/pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json
    O arquivo usa o formato JSON Lines (um objeto JSON por linha), que e o
    formato lido nativamente pelo Amazon Athena com o JSON SerDe.

Camada Silver
    Enriquecimento via JOIN (pedidos validos + clientes + produtos),
    calculo de valor_total = quantidade * preco e persistencia em
    Parquet com compressao Snappy em:
        processed/fato_vendas/ingest_date=YYYY-MM-DD/
"""

from __future__ import annotations

import json

import pandas as pd

from src.config import settings
from src.storage import Storage, get_storage

MOTIVOS = {
    "R1": "QUANTIDADE_NAO_NUMERICA: valor de quantidade nao pode ser convertido para inteiro",
    "R2": "QUANTIDADE_INVALIDA: quantidade menor ou igual a zero",
    "R3": "CLIENTE_INEXISTENTE: cliente_id nao encontrado na dimensao clientes",
    "R4": "PRODUTO_INEXISTENTE: product_id nao encontrado na dimensao produtos",
}


def aplicar_data_quality(
    pedidos: pd.DataFrame, clientes: pd.DataFrame, produtos: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retorna (pedidos_validos, pedidos_rejeitados_com_motivos)."""
    df = pedidos.copy()

    quantidade_num = pd.to_numeric(df["quantidade"], errors="coerce")
    ids_clientes = set(clientes["cliente_id"])
    ids_produtos = set(produtos["product_id"])

    falha_r1 = quantidade_num.isna()
    falha_r2 = (~falha_r1) & (quantidade_num <= 0)
    falha_r3 = ~df["cliente_id"].isin(ids_clientes)
    falha_r4 = ~df["product_id"].isin(ids_produtos)

    # Um mesmo pedido pode violar mais de uma regra: acumulamos todos os motivos
    # em uma lista, garantindo que cada registro rejeitado apareca UMA unica vez.
    motivos_por_linha = []
    for r1, r2, r3, r4 in zip(falha_r1, falha_r2, falha_r3, falha_r4):
        motivos = []
        if r1:
            motivos.append(MOTIVOS["R1"])
        if r2:
            motivos.append(MOTIVOS["R2"])
        if r3:
            motivos.append(MOTIVOS["R3"])
        if r4:
            motivos.append(MOTIVOS["R4"])
        motivos_por_linha.append(motivos)

    df["_motivos"] = motivos_por_linha
    reprovado = df["_motivos"].str.len() > 0

    rejeitados = df[reprovado].copy()
    validos = df[~reprovado].drop(columns=["_motivos"]).copy()
    validos["quantidade"] = quantidade_num[~reprovado].astype("int64")

    return validos, rejeitados


def montar_quarentena(rejeitados: pd.DataFrame) -> str:
    """Serializa os registros rejeitados em JSON Lines com o motivo da rejeicao."""
    linhas = []
    for _, row in rejeitados.iterrows():
        registro = {
            "pedido_id": row["pedido_id"],
            "cliente_id": row["cliente_id"],
            "product_id": row["product_id"],
            "quantidade": row["quantidade"],
            "data_pedido": row["data_pedido"],
            "canal_venda": row["canal_venda"],
            "motivo_rejeicao": "; ".join(row["_motivos"]),
            "qtd_regras_violadas": len(row["_motivos"]),
            "ingest_date": settings.ingest_date,
        }
        linhas.append(json.dumps(registro, ensure_ascii=False))
    return "\n".join(linhas) + ("\n" if linhas else "")


def construir_silver(
    validos: pd.DataFrame, clientes: pd.DataFrame, produtos: pd.DataFrame
) -> pd.DataFrame:
    fato = (
        validos.merge(clientes, on="cliente_id", how="inner", validate="many_to_one")
        .merge(produtos, on="product_id", how="inner", validate="many_to_one")
    )
    fato["valor_total"] = (fato["quantidade"] * fato["preco"]).round(2)

    colunas = [
        "pedido_id",
        "data_pedido",
        "canal_venda",
        "cliente_id",
        "nome_cliente",
        "uf",
        "cidade",
        "segmento",
        "product_id",
        "nome_produto",
        "categoria",
        "fornecedor",
        "preco",
        "quantidade",
        "valor_total",
    ]
    return fato[colunas].sort_values("pedido_id").reset_index(drop=True)


def executar(storage: Storage | None = None) -> dict:
    storage = storage or get_storage()

    clientes = storage.read_csv(settings.key_raw("clientes"), dtype={"cliente_id": str})
    produtos = storage.read_csv(settings.key_raw("produtos"), dtype={"product_id": str})
    pedidos = storage.read_csv(
        settings.key_raw("pedidos"), dtype={"cliente_id": str, "product_id": str}
    )

    validos, rejeitados = aplicar_data_quality(pedidos, clientes, produtos)

    destino_quarentena = storage.write_text(
        settings.key_quarantine(), montar_quarentena(rejeitados)
    )

    fato_vendas = construir_silver(validos, clientes, produtos)
    destino_silver = storage.write_parquet(settings.key_silver(), fato_vendas)

    # Contagem por motivo (um pedido com 2 violacoes conta nos 2 motivos)
    por_motivo: dict[str, int] = {}
    for motivos in rejeitados["_motivos"]:
        for m in motivos:
            chave = m.split(":")[0]
            por_motivo[chave] = por_motivo.get(chave, 0) + 1

    print("\n=== ETAPA 2 | DATA QUALITY, QUARENTENA E SILVER ===")
    print(f"pedidos lidos da Raw : {len(pedidos):>6}")
    print(f"pedidos aprovados    : {len(fato_vendas):>6}  -> {destino_silver}")
    print(f"pedidos rejeitados   : {len(rejeitados):>6}  -> {destino_quarentena}")
    print("rejeicoes por regra:")
    for chave, qtd in sorted(por_motivo.items()):
        print(f"  - {chave:<26}: {qtd}")
    print(f"conciliacao: {len(pedidos)} raw = {len(fato_vendas)} silver + {len(rejeitados)} quarentena "
          f"-> {'OK' if len(pedidos) == len(fato_vendas) + len(rejeitados) else 'DIVERGENTE'}")

    return {
        "pedidos_raw": int(len(pedidos)),
        "pedidos_silver": int(len(fato_vendas)),
        "pedidos_quarentena": int(len(rejeitados)),
        "rejeicoes_por_regra": por_motivo,
        "receita_total_silver": float(fato_vendas["valor_total"].sum()),
        "destinos": {"silver": destino_silver, "quarentena": destino_quarentena},
    }


if __name__ == "__main__":
    executar()
