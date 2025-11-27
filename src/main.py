# src/main.py
from config.settings import Config
from logging_handler.logging_handler import LoggingConfig
from session.spark_session import SparkSessionManager
from pipeline.pipeline import Pipeline
import logging


def main():
    """
    Função principal que atua como a "Raiz de Composição".
    Configura e executa o pipeline.
    """
    config = Config()
    logging_config = LoggingConfig()

    config_dict = config.carregar_config()
    logging_config.configurar_logging()

    app_name = config_dict["spark"]["app_name"]

    spark = None
    try:
        spark = SparkSessionManager.get_spark_session(app_name=app_name)

        pipeline = Pipeline(spark)
        pipeline.run(config=config_dict)
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado na execução do programa: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    main()
