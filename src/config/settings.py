# src/config/settings.py
import logging
import yaml

logger = logging.getLogger(__name__)


def carregar_config(path: str = "src/config/settings.yaml") -> dict:
    """Carrega um arquivo de configuração YAML."""
    with open(path, "r") as file:
        return yaml.safe_load(file)
