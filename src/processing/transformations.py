# src/processing/transformations.py
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, year
from typing import Tuple


class Transformation:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _filtrar_pagamentos(self, pagamentos):
        try:
            pagamentos_por_status = pagamentos.filter(
                (col("status") == False) & (col("avaliacao_fraude.fraude") == False)
            )

            df_final = pagamentos_por_status.withColumnRenamed(
                "id_pedido", "id_pedido_pagamento"
            )
            return df_final
        except Exception as e:
            logging.exception(e)
            raise

    def _filtrar_pedidos(self, pedidos):
        try:
            pedidos_2025 = pedidos.filter(year(col("data_criacao")) == 2025)

            pedidos_valor_total = pedidos_2025.withColumn(
                "valor_total", col("valor_unitario") * col("quantidade")
            )
            df_final = pedidos_valor_total.withColumnRenamed(
                "id_pedido", "id_pedido_pedido"
            )
            return df_final
        except Exception as e:
            logging.exception(e)
            raise

    def _join_dataframes(self, pagamentos, pedidos):
        try:
            join = pagamentos.join(
                pedidos,
                pagamentos.id_pedido_pagamento == pedidos.id_pedido_pedido,
                "inner",
            )

            result = join.select(
                col("id_pedido_pagamento").alias("id_pedido"),
                col("uf"),
                col("forma_pagamento"),
                col("valor_total"),
                col("data_criacao"),
            ).orderBy("uf", "forma_pagamento", "data_criacao")
            return result
        except Exception as e:
            logging.exception(e)
            raise

    def run(self, pagamentos: DataFrame, pedidos: DataFrame) -> DataFrame:
        try:
            logging.info("Trasnformação de dados")

            pagamentos = self._filtrar_pagamentos(pagamentos)
            pedidos = self._filtrar_pedidos(pedidos)

            result = self._join_dataframes(pagamentos=pagamentos, pedidos=pedidos)

            logging.info("Transoformação de dados ocorreu com sucesso")
            return result

        except Exception as e:
            logging.exception(f"Erro na criação do relatório {str(e)}")
            raise
