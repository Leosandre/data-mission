"""Módulo de validação — verifica qualidade dos dados finais."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def validate(df: pd.DataFrame, indicators: pd.DataFrame = None) -> dict:
    """Executa validações de qualidade e retorna relatório."""
    report = {}

    # 1. Completude — campos obrigatórios sem nulos
    null_pct = df[["log_id", "timestamp", "status_code"]].isnull().mean() * 100
    report["null_pct_campos_obrigatorios"] = {k: (v if pd.notna(v) else 0.0) for k, v in null_pct.to_dict().items()}

    # 2. Unicidade — log_id deve ser único
    duplicates = df["log_id"].duplicated().sum()
    report["log_id_duplicados"] = int(duplicates)

    # 3. Volume
    report["total_registros"] = len(df)

    # 4. Distribuição de status
    report["status_distribution"] = df["status_category"].value_counts().to_dict() if not df.empty else {}

    # 5. Taxa de erro
    error_rate = df["is_error"].mean() * 100 if not df.empty else 0.0
    report["error_rate_pct"] = round(error_rate, 2)

    # 6. Latência
    def safe_int(val):
        return int(val) if pd.notna(val) else None

    report["latency_p50_ms"] = safe_int(df["response_time_ms"].median())
    report["latency_p95_ms"] = safe_int(df["response_time_ms"].quantile(0.95))
    report["latency_p99_ms"] = safe_int(df["response_time_ms"].quantile(0.99))

    # 7. Range temporal
    report["timestamp_min"] = str(df["timestamp"].min())
    report["timestamp_max"] = str(df["timestamp"].max())

    # 8. Classificação de sinistros
    if "incident_severity" in df.columns and not df.empty:
        report["incident_distribution"] = df["incident_severity"].value_counts().to_dict()

    # 9. Indicadores
    if indicators is not None:
        report["indicator_days"] = len(indicators)

    # Resultado
    null_max = max(report["null_pct_campos_obrigatorios"].values(), default=0.0)
    passed = duplicates == 0 and null_max == 0 and len(df) > 0
    report["validation_passed"] = passed

    status = "✅ PASSED" if passed else "❌ FAILED"
    logger.info(f"Validação {status}")
    for key, val in report.items():
        logger.info(f"  {key}: {val}")

    return report
