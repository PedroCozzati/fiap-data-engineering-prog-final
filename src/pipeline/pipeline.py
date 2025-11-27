# src/pipeline/pipeline.py
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import config.settings as settings

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_handler = DataHandler(self.spark)
        self.transformer = Transformation(self.spark)

    def run(self, config):
        logger.info("Inicio do processo")
        pagamentos = self.data_handler.get_pagamentos(config)
        logging.info(pagamentos.show(5, truncate=False))

        pedidos = self.data_handler.get_pedidos(config)
        logging.info(pedidos.show(5, truncate=False))

        transformed = self.transformer.run(pagamentos, pedidos)
        logger.info(transformed.show(5, truncate=False))

        self.data_handler.send_output(transformed, config)
        logger.info("Processo finalizodo")
