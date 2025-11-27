# tests/test_data_handler.py
import pytest
import os
from pyspark.sql import SparkSession
from src.io_utils.data_handler import DataHandler
from src.config.settings import Config


@pytest.fixture(scope="module")
def spark():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    A sessão é finalizada automaticamente ao final da execução dos testes.
    """
    spark = (
        SparkSession.builder.appName("PySpark Unit Tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def data_handler(spark):
    return DataHandler(spark)


def test_get_pagamentos(tmp_path, data_handler):
    pagamentos_file = tmp_path / "pagamentos.json"
    pagamentos_file.write_text(
        """
    [
        {
            "id_pedido": "123",
            "forma_pagamento": "cartao",
            "valor_pagamento": 100.5,
            "status": true,
            "data_processamento": "2025-11-27",
            "avaliacao_fraude": {
                "fraude": false,
                "score": 0.1
            }
        }
    ]
    """
    )
    config = {
        "file_options": {"pagamentos_json": {"json_multiline": True}},
        "paths": {"pagamentos": str(pagamentos_file)},
    }

    df = data_handler.get_pagamentos(config)
    result = df.collect()
    assert len(result) == 1
    assert result[0]["id_pedido"] == "123"
    assert result[0]["avaliacao_fraude"]["fraude"] is False


def test_get_pedidos(tmp_path, data_handler):
    pedidos_file = tmp_path / "pedidos.csv"
    pedidos_file.write_text(
        "id_pedido,produto,valor_unitario,quantidade,data_criacao,uf,id_cliente\n"
        "123,Notebook,2500.0,2,2025-11-27,SP,999\n"
    )
    config = {
        "file_options": {"pedidos_csv": {"header": True, "sep": ","}},
        "paths": {"pedidos": str(pedidos_file)},
    }

    df = data_handler.get_pedidos(config)
    result = df.collect()
    assert len(result) == 1
    assert result[0]["produto"] == "Notebook"
    assert result[0]["quantidade"] == 2


def test_send_output(tmp_path, data_handler):
    df = data_handler.spark.createDataFrame(
        [("123", "Notebook")], ["id_pedido", "produto"]
    )
    output_dir = tmp_path / "output"
    config = {"paths": {"output": str(output_dir)}}

    data_handler.send_output(df, config, mode="overwrite")

    files = os.listdir(output_dir)
    assert any(f.endswith(".parquet") or f.startswith("part-") for f in files)
