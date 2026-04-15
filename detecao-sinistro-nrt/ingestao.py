"""Módulo de ingestão — coleta dados da API DataMission e persiste em staging."""

import logging
import os
from datetime import datetime

import requests

from config import API_URL, STAGING_DIR, LOGS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "ingestao.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def get_token() -> str:
    """Obtém token da variável de ambiente ou config."""
    token = os.environ.get("DATAMISSION_TOKEN")
    if not token:
        from config import API_TOKEN
        token = API_TOKEN
    if token == "SEU_TOKEN_AQUI":
        raise ValueError("Configure DATAMISSION_TOKEN como variável de ambiente")
    return token


def fetch_dataset(token: str) -> bytes:
    """Faz GET na API e retorna o conteúdo CSV."""
    headers = {"Authorization": f"Bearer {token}"}
    logger.info(f"Requisição GET para {API_URL}")

    response = requests.get(API_URL, headers=headers, timeout=60)
    response.raise_for_status()

    logger.info(f"Status: {response.status_code} | Tamanho: {len(response.content)} bytes")
    return response.content


def save_to_staging(content: bytes) -> str:
    """Salva CSV bruto em staging com timestamp."""
    os.makedirs(STAGING_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(STAGING_DIR, f"raw_{timestamp}.csv")

    with open(filepath, "wb") as f:
        f.write(content)

    logger.info(f"Arquivo salvo em staging: {filepath}")
    return filepath


def ingest() -> str:
    """Executa o pipeline de ingestão completo. Retorna o path do arquivo."""
    logger.info("=== INÍCIO DA INGESTÃO ===")
    try:
        token = get_token()
        content = fetch_dataset(token)
        filepath = save_to_staging(content)
        logger.info(f"=== INGESTÃO CONCLUÍDA: {filepath} ===")
        return filepath
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP na API: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro na ingestão: {e}")
        raise


if __name__ == "__main__":
    ingest()
