# src/io_utils/data_handler.py
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType,
    FloatType,
    DateType,
    LongType,
)
from typing import Tuple
import os
from pyspark.sql import DataFrame


class DataHandler:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_pagamentos_schema(self) -> StructType:
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("valor_pagamento", FloatType(), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", DateType(), True),
                StructField(
                    "avaliacao_fraude",
                    StructType(
                        [
                            StructField("fraude", BooleanType(), True),
                            StructField("score", FloatType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    def _get_pedidos_schema(self) -> StructType:
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", DateType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )

    def get_pagamentos(self, config):
        try:
            get_multiline_str = str(
                config["file_options"]["pagamentos_json"]["json_multiline"]
            ).lower()
            pagamentos_path = config["paths"]["pagamentos"]
            schema = self._get_pagamentos_schema()
            pagamentos_df = (
                self.spark.read.schema(schema)
                .option("multiLine", get_multiline_str)
                .json(pagamentos_path)
            )
            return pagamentos_df
        except Exception as e:
            logging.error(e)
            raise

    def get_pedidos(self, config):
        try:
            get_header_str = str(
                config["file_options"]["pedidos_csv"]["header"]
            ).lower()
            get_sep_str = str(config["file_options"]["pedidos_csv"]["sep"]).lower()
            pedidos_path = config["paths"]["pedidos"]
            schema = self._get_pedidos_schema()

            pedidos_df = (
                self.spark.read.schema(schema)
                .option("header", get_header_str)
                .option("sep", get_sep_str)
                .csv(pedidos_path)
            )
            logging.info(pedidos_df.show(5, truncate=False))
            return pedidos_df
        except Exception as e:
            logging.exception(e)
            raise

    def send_output(self, df: DataFrame, config, mode: str = "overwrite"):
        try:
            output = config["paths"]["output"]
            df.write.mode(mode).parquet(output)
        except Exception as e:
            logging.exception(e)
            raise
