"""
Testes das regras de Data Quality.

Execucao:
    python -m tests.test_data_quality
ou, se tiver pytest instalado:
    pytest -q
"""

from __future__ import annotations

import pandas as pd

from src.process_silver import aplicar_data_quality, construir_silver

CLIENTES = pd.DataFrame(
    [
        {"cliente_id": "C00001", "nome_cliente": "Ana Silva", "uf": "SP",
         "cidade": "Sao Paulo", "segmento": "Varejo", "data_cadastro": "2023-01-10"},
        {"cliente_id": "C00002", "nome_cliente": "Bruno Costa", "uf": "RJ",
         "cidade": "Rio de Janeiro", "segmento": "Atacado", "data_cadastro": "2023-05-02"},
    ]
)

PRODUTOS = pd.DataFrame(
    [
        {"product_id": "P00001", "nome_produto": "Notebook", "categoria": "Informatica",
         "preco": 100.0, "fornecedor": "Alfa"},
        {"product_id": "P00002", "nome_produto": "Cadeira", "categoria": "Moveis",
         "preco": 250.0, "fornecedor": "Beta"},
    ]
)

PEDIDOS = pd.DataFrame(
    [
        # valido
        {"pedido_id": "PED1", "cliente_id": "C00001", "product_id": "P00001",
         "quantidade": 3, "data_pedido": "2026-09-01", "canal_venda": "E-commerce"},
        # quantidade negativa
        {"pedido_id": "PED2", "cliente_id": "C00001", "product_id": "P00002",
         "quantidade": -5, "data_pedido": "2026-09-01", "canal_venda": "Loja Fisica"},
        # quantidade zero
        {"pedido_id": "PED3", "cliente_id": "C00002", "product_id": "P00001",
         "quantidade": 0, "data_pedido": "2026-09-02", "canal_venda": "Televendas"},
        # cliente inexistente
        {"pedido_id": "PED4", "cliente_id": "C99999", "product_id": "P00001",
         "quantidade": 2, "data_pedido": "2026-09-02", "canal_venda": "Marketplace"},
        # produto inexistente
        {"pedido_id": "PED5", "cliente_id": "C00002", "product_id": "P99999",
         "quantidade": 4, "data_pedido": "2026-09-03", "canal_venda": "E-commerce"},
        # duas violacoes ao mesmo tempo
        {"pedido_id": "PED6", "cliente_id": "C99998", "product_id": "P00002",
         "quantidade": -1, "data_pedido": "2026-09-03", "canal_venda": "E-commerce"},
        # valido
        {"pedido_id": "PED7", "cliente_id": "C00002", "product_id": "P00002",
         "quantidade": 2, "data_pedido": "2026-09-04", "canal_venda": "Loja Fisica"},
    ]
)


def test_separacao_validos_e_rejeitados():
    validos, rejeitados = aplicar_data_quality(PEDIDOS, CLIENTES, PRODUTOS)
    assert list(validos["pedido_id"]) == ["PED1", "PED7"]
    assert sorted(rejeitados["pedido_id"]) == ["PED2", "PED3", "PED4", "PED5", "PED6"]


def test_conciliacao_sem_perda_de_registros():
    validos, rejeitados = aplicar_data_quality(PEDIDOS, CLIENTES, PRODUTOS)
    assert len(PEDIDOS) == len(validos) + len(rejeitados)


def test_registro_com_duas_violacoes_aparece_uma_unica_vez():
    _, rejeitados = aplicar_data_quality(PEDIDOS, CLIENTES, PRODUTOS)
    ped6 = rejeitados[rejeitados["pedido_id"] == "PED6"]
    assert len(ped6) == 1
    assert len(ped6.iloc[0]["_motivos"]) == 2


def test_calculo_do_valor_total():
    validos, _ = aplicar_data_quality(PEDIDOS, CLIENTES, PRODUTOS)
    fato = construir_silver(validos, CLIENTES, PRODUTOS)
    esperado = {"PED1": 3 * 100.0, "PED7": 2 * 250.0}
    for pedido_id, valor in esperado.items():
        assert float(fato.loc[fato["pedido_id"] == pedido_id, "valor_total"].iloc[0]) == valor


def test_nenhuma_anomalia_na_silver():
    validos, _ = aplicar_data_quality(PEDIDOS, CLIENTES, PRODUTOS)
    fato = construir_silver(validos, CLIENTES, PRODUTOS)
    assert (fato["quantidade"] > 0).all()
    assert fato["cliente_id"].isin(CLIENTES["cliente_id"]).all()
    assert fato["product_id"].isin(PRODUTOS["product_id"]).all()


if __name__ == "__main__":
    testes = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for teste in testes:
        teste()
        print(f"OK  {teste.__name__}")
    print(f"\n{len(testes)} testes aprovados.")
