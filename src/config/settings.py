# src/config/settings.py
import logging
import yaml

logger = logging.getLogger(__name__)


class Config:

    def carregar_config(self) -> dict:
        """Carrega um arquivo de configuração YAML."""
        path: str = "src/config/settings.yaml"
        with open(path, "r") as file:
            return yaml.safe_load(file)
