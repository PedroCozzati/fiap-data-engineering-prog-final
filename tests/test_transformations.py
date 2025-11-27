# tests/test_processing.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DoubleType,
    TimestampType,
)
from datetime import datetime
from src.processing.transformations import Transformation
from pyspark.sql import Row
from src.config.settings import carregar_config

config = carregar_config()


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


def test_transformations(spark):
    transformer = Transformation(spark)

    pagamentos_data = [
        Row(
            id_pedido="929292921",
            forma_pagamento="cartao de credito",
            valor_pagamento=200.0,
            status=False,
            data_processamento=datetime(2025, 9, 17),
            avaliacao_fraude=Row(fraude=True, score=0.2),
        ),
        Row(
            id_pedido="9yrtyrt921",
            forma_pagamento="pix",
            valor_pagamento=2020.0,
            status=False,
            data_processamento=datetime(2025, 2, 17),
            avaliacao_fraude=Row(fraude=False, score=0.2),
        ),
        Row(
            id_pedido="92ewrwrw21",
            forma_pagamento="cartao de credito",
            valor_pagamento=2100.0,
            status=False,
            data_processamento=datetime(2025, 1, 17),
            avaliacao_fraude=Row(fraude=False, score=0.2),
        ),
    ]
    pagamentos_df = spark.createDataFrame(pagamentos_data)

    pedidos_data = [
        Row(
            id_pedido="5435345rgfd1",
            produto="TESTE",
            valor_unitario=200.0,
            quantidade=1,
            data_criacao=datetime(2025, 9, 3),
            uf="SP",
            cliente_id="a9999",
        ),
        Row(
            id_pedido="92ewrwrw21",
            produto="TESTE",
            valor_unitario=20.0,
            quantidade=5,
            data_criacao=datetime(2025, 1, 17),
            uf="SP",
            cliente_id="dasdas",
        ),
        Row(
            id_pedido="dsf89767",
            produto="TESTE4",
            valor_unitario=60.0,
            quantidade=12,
            data_criacao=datetime(2025, 9, 3),
            uf="AL",
            cliente_id="adas",
        ),
        Row(
            id_pedido="lkj9080080",
            produto="TESTE3",
            valor_unitario=20.0,
            quantidade=11,
            data_criacao=datetime(2025, 9, 3),
            uf="SP",
            cliente_id="adadasd",
        ),
    ]
    pedidos_df = spark.createDataFrame(pedidos_data)
    result = transformer.run(pagamentos_df, pedidos_df)
    rows = result.collect()
    assert rows[0]["id_pedido"] == "92ewrwrw21"
