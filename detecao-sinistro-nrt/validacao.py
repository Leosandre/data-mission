"""Módulo de validação — verifica qualidade dos dados finais."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def validate(df: pd.DataFrame) -> dict:
    """Executa validações de qualidade e retorna relatório."""
    report = {}

    # 1. Completude — campos obrigatórios sem nulos
    null_pct = df[["log_id", "timestamp", "status_code"]].isnull().mean() * 100
    report["null_pct_campos_obrigatorios"] = null_pct.to_dict()

    # 2. Unicidade — log_id deve ser único
    duplicates = df["log_id"].duplicated().sum()
    report["log_id_duplicados"] = int(duplicates)

    # 3. Volume
    report["total_registros"] = len(df)

    # 4. Distribuição de status
    report["status_distribution"] = df["status_category"].value_counts().to_dict()

    # 5. Taxa de erro
    error_rate = df["is_error"].mean() * 100
    report["error_rate_pct"] = round(error_rate, 2)

    # 6. Latência
    report["latency_p50_ms"] = int(df["response_time_ms"].median())
    report["latency_p95_ms"] = int(df["response_time_ms"].quantile(0.95))
    report["latency_p99_ms"] = int(df["response_time_ms"].quantile(0.99))

    # 7. Range temporal
    report["timestamp_min"] = str(df["timestamp"].min())
    report["timestamp_max"] = str(df["timestamp"].max())

    # Resultado
    passed = duplicates == 0 and null_pct.max() == 0 and len(df) > 0
    report["validation_passed"] = passed

    status = "✅ PASSED" if passed else "❌ FAILED"
    logger.info(f"Validação {status}")
    for key, val in report.items():
        logger.info(f"  {key}: {val}")

    return report
