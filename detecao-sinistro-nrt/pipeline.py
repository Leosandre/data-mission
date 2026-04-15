"""Pipeline principal — orquestra ingestão, transformação e validação."""

import logging
import os
import sys

from config import LOGS_DIR

os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "pipeline.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def run(source: str = None):
    """Executa o pipeline completo.

    Args:
        source: Path de um CSV local. Se None, busca da API.
    """
    from ingestao import ingest
    from transformacao import transform
    from validacao import validate

    logger.info("=" * 60)
    logger.info("PIPELINE DE DETECÇÃO DE SINISTROS — HOSPITAL COSTELA")
    logger.info("=" * 60)

    # 1. Ingestão
    if source and os.path.exists(source):
        logger.info(f"Usando arquivo local: {source}")
        filepath = source
    else:
        filepath = ingest()

    # 2. Transformação
    df = transform(filepath)

    # 3. Validação
    report = validate(df)

    if not report["validation_passed"]:
        logger.warning("Pipeline concluído com falhas de validação")
        return 1

    logger.info("Pipeline concluído com sucesso ✅")
    return 0


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else None
    exit_code = run(source)
    sys.exit(exit_code)
