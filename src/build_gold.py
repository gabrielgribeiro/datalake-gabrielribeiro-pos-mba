"""
ETAPA 3 - CAMADA GOLD

Le a camada Silver (fato_vendas) e produz a agregacao analitica por
UF e CATEGORIA, com metricas de negocio, persistida em Parquet/Snappy em:

    gold/agg_vendas_uf_categoria/ingest_date=YYYY-MM-DD/

Metricas calculadas:
    qtd_pedidos        - numero de pedidos validos
    qtd_itens          - soma das quantidades vendidas
    receita_total      - soma de valor_total
    ticket_medio       - receita_total / qtd_pedidos
    preco_medio_item   - preco medio praticado
    clientes_distintos - clientes unicos que compraram
    produtos_distintos - SKUs unicos vendidos
"""

from __future__ import annotations

import pandas as pd

from src.config import settings
from src.storage import Storage, get_storage


def agregar(fato: pd.DataFrame) -> pd.DataFrame:
    gold = (
        fato.groupby(["uf", "categoria"], as_index=False)
        .agg(
            qtd_pedidos=("pedido_id", "count"),
            qtd_itens=("quantidade", "sum"),
            receita_total=("valor_total", "sum"),
            preco_medio_item=("preco", "mean"),
            clientes_distintos=("cliente_id", "nunique"),
            produtos_distintos=("product_id", "nunique"),
        )
    )
    gold["ticket_medio"] = (gold["receita_total"] / gold["qtd_pedidos"]).round(2)
    gold["receita_total"] = gold["receita_total"].round(2)
    gold["preco_medio_item"] = gold["preco_medio_item"].round(2)

    colunas = [
        "uf",
        "categoria",
        "qtd_pedidos",
        "qtd_itens",
        "receita_total",
        "ticket_medio",
        "preco_medio_item",
        "clientes_distintos",
        "produtos_distintos",
    ]
    return gold[colunas].sort_values(["uf", "categoria"]).reset_index(drop=True)


def executar(storage: Storage | None = None) -> dict:
    storage = storage or get_storage()

    fato = storage.read_parquet(settings.key_silver())
    gold = agregar(fato)
    destino = storage.write_parquet(settings.key_gold(), gold)

    top = gold.sort_values("receita_total", ascending=False).head(5)

    print("\n=== ETAPA 3 | CAMADA GOLD ===")
    print(f"linhas agregadas (uf x categoria): {len(gold)}  -> {destino}")
    print(f"receita total consolidada        : R$ {gold['receita_total'].sum():,.2f}")
    print("top 5 combinacoes por receita:")
    for _, row in top.iterrows():
        print(
            f"  - {row['uf']} / {row['categoria']:<12}: "
            f"R$ {row['receita_total']:>14,.2f} | {int(row['qtd_pedidos'])} pedidos"
        )

    return {
        "linhas_gold": int(len(gold)),
        "receita_total_gold": float(gold["receita_total"].sum()),
        "destino": destino,
    }


if __name__ == "__main__":
    executar()
