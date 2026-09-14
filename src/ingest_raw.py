"""
ETAPA 1 - INGESTAO (CAMADA RAW)

Gera a massa de dados simulada (clientes, produtos e pedidos) contendo
anomalias intencionais e grava em CSV delimitado por virgula, particionado
por data de ingestao no padrao Hive:

    raw/clientes/ingest_date=YYYY-MM-DD/clientes.csv
    raw/produtos/ingest_date=YYYY-MM-DD/produtos.csv
    raw/pedidos/ingest_date=YYYY-MM-DD/pedidos.csv

Anomalias injetadas de proposito nos pedidos:
    1. quantidade negativa
    2. quantidade igual a zero
    3. cliente_id inexistente na dimensao clientes
    4. product_id inexistente na dimensao produtos

A geracao usa uma semente fixa (SEED), portanto o resultado e reproduzivel.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import pandas as pd

from src.config import settings
from src.storage import Storage, get_storage

# ---------------------------------------------------------------- catalogos base
UFS = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "GO", "DF", "ES"]
CIDADES = {
    "SP": ["Sao Paulo", "Campinas", "Santos"],
    "RJ": ["Rio de Janeiro", "Niteroi", "Petropolis"],
    "MG": ["Belo Horizonte", "Uberlandia", "Juiz de Fora"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas"],
    "PR": ["Curitiba", "Londrina", "Maringa"],
    "SC": ["Florianopolis", "Joinville", "Blumenau"],
    "BA": ["Salvador", "Feira de Santana", "Ilheus"],
    "PE": ["Recife", "Olinda", "Caruaru"],
    "CE": ["Fortaleza", "Sobral", "Juazeiro do Norte"],
    "GO": ["Goiania", "Anapolis", "Rio Verde"],
    "DF": ["Brasilia", "Taguatinga", "Ceilandia"],
    "ES": ["Vitoria", "Vila Velha", "Serra"],
}
SEGMENTOS = ["Varejo", "Atacado", "Corporativo", "Governo"]
CATEGORIAS = ["Eletronicos", "Moveis", "Vestuario", "Alimentos", "Informatica", "Papelaria"]
# faixa de preco por categoria, para que a massa simulada tenha aparencia realista
FAIXA_PRECO = {
    "Eletronicos": (350.0, 6500.0),
    "Moveis": (180.0, 3200.0),
    "Vestuario": (35.0, 480.0),
    "Alimentos": (5.0, 120.0),
    "Informatica": (90.0, 9000.0),
    "Papelaria": (3.0, 95.0),
}
FORNECEDORES = ["Fornecedor Alfa", "Fornecedor Beta", "Fornecedor Gama", "Fornecedor Delta"]
CANAIS = ["Loja Fisica", "E-commerce", "Televendas", "Marketplace"]
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Heitor",
         "Isabela", "Joao", "Karina", "Lucas", "Mariana", "Nicolas", "Olivia", "Pedro"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Pereira", "Costa", "Almeida",
              "Ribeiro", "Carvalho", "Gomes", "Martins", "Rocha"]


def gerar_clientes(rng: random.Random, n: int) -> pd.DataFrame:
    linhas = []
    base = date(2022, 1, 1)
    for i in range(1, n + 1):
        uf = rng.choice(UFS)
        linhas.append(
            {
                "cliente_id": f"C{i:05d}",
                "nome_cliente": f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}",
                "uf": uf,
                "cidade": rng.choice(CIDADES[uf]),
                "segmento": rng.choice(SEGMENTOS),
                "data_cadastro": (base + timedelta(days=rng.randint(0, 1200))).isoformat(),
            }
        )
    return pd.DataFrame(linhas)


def gerar_produtos(rng: random.Random, n: int) -> pd.DataFrame:
    linhas = []
    for i in range(1, n + 1):
        categoria = rng.choice(CATEGORIAS)
        preco_min, preco_max = FAIXA_PRECO[categoria]
        linhas.append(
            {
                "product_id": f"P{i:05d}",
                "nome_produto": f"{categoria} Modelo {i:03d}",
                "categoria": categoria,
                "preco": round(rng.uniform(preco_min, preco_max), 2),
                "fornecedor": rng.choice(FORNECEDORES),
            }
        )
    return pd.DataFrame(linhas)


def gerar_pedidos(
    rng: random.Random,
    clientes: pd.DataFrame,
    produtos: pd.DataFrame,
    n: int,
    taxa_anomalias: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Gera pedidos validos e injeta anomalias controladas."""
    ids_clientes = clientes["cliente_id"].tolist()
    ids_produtos = produtos["product_id"].tolist()

    qtd_anomalias = int(n * taxa_anomalias)
    posicoes_anomalas = set(rng.sample(range(n), qtd_anomalias))
    tipos = ["quantidade_negativa", "quantidade_zero", "cliente_inexistente", "produto_inexistente"]

    contador = {t: 0 for t in tipos}
    contador["valido"] = 0

    base = date.fromisoformat(settings.ingest_date) - timedelta(days=30)
    linhas = []
    for i in range(n):
        pedido = {
            "pedido_id": f"PED{i + 1:06d}",
            "cliente_id": rng.choice(ids_clientes),
            "product_id": rng.choice(ids_produtos),
            "quantidade": rng.randint(1, 25),
            "data_pedido": (base + timedelta(days=rng.randint(0, 30))).isoformat(),
            "canal_venda": rng.choice(CANAIS),
        }

        if i in posicoes_anomalas:
            tipo = rng.choice(tipos)
            if tipo == "quantidade_negativa":
                pedido["quantidade"] = -rng.randint(1, 10)
            elif tipo == "quantidade_zero":
                pedido["quantidade"] = 0
            elif tipo == "cliente_inexistente":
                pedido["cliente_id"] = f"C9{rng.randint(1000, 9999)}"
            else:
                pedido["product_id"] = f"P9{rng.randint(1000, 9999)}"
            contador[tipo] += 1
        else:
            contador["valido"] += 1

        linhas.append(pedido)

    return pd.DataFrame(linhas), contador


def executar(storage: Storage | None = None) -> dict:
    storage = storage or get_storage()
    rng = random.Random(settings.seed)

    clientes = gerar_clientes(rng, settings.n_clientes)
    produtos = gerar_produtos(rng, settings.n_produtos)
    pedidos, contador = gerar_pedidos(
        rng, clientes, produtos, settings.n_pedidos, settings.taxa_anomalias
    )

    destinos = {
        "clientes": storage.write_csv(settings.key_raw("clientes"), clientes),
        "produtos": storage.write_csv(settings.key_raw("produtos"), produtos),
        "pedidos": storage.write_csv(settings.key_raw("pedidos"), pedidos),
    }

    print("\n=== ETAPA 1 | INGESTAO RAW ===")
    print(f"clientes gravados : {len(clientes):>6}  -> {destinos['clientes']}")
    print(f"produtos gravados : {len(produtos):>6}  -> {destinos['produtos']}")
    print(f"pedidos gravados  : {len(pedidos):>6}  -> {destinos['pedidos']}")
    print("anomalias injetadas de proposito:")
    for tipo in ("quantidade_negativa", "quantidade_zero", "cliente_inexistente", "produto_inexistente"):
        print(f"  - {tipo:<22}: {contador[tipo]}")

    return {
        "clientes": len(clientes),
        "produtos": len(produtos),
        "pedidos": len(pedidos),
        "anomalias_injetadas": contador,
        "destinos": destinos,
    }


if __name__ == "__main__":
    executar()
