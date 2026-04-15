"""Módulo de transformação — validação de schema, limpeza, enriquecimento e regras de negócio."""

import logging
import os

import pandas as pd

from config import EXPECTED_COLUMNS, VALID_HTTP_METHODS, OUTPUT_DIR

logger = logging.getLogger(__name__)


def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Valida se o DataFrame tem as colunas esperadas."""
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes no dataset: {missing}")

    extra = set(df.columns) - set(EXPECTED_COLUMNS)
    if extra:
        logger.warning(f"Colunas extras ignoradas: {extra}")

    return df[EXPECTED_COLUMNS]


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Limpeza: tipos, nulos, valores inválidos."""
    initial_count = len(df)

    # Remover duplicatas por log_id
    df = df.drop_duplicates(subset="log_id")

    # Remover registros com campos obrigatórios nulos
    df = df.dropna(subset=["log_id", "timestamp", "status_code"])

    # Converter tipos
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["status_code"] = pd.to_numeric(df["status_code"], errors="coerce").astype("Int64")
    df["response_time_ms"] = pd.to_numeric(df["response_time_ms"], errors="coerce").astype("Int64")

    # Remover registros com timestamp inválido
    df = df.dropna(subset=["timestamp"])

    # Filtrar métodos HTTP válidos
    df = df[df["http_method"].isin(VALID_HTTP_METHODS)]

    # Normalizar endpoint (lowercase, strip)
    df["endpoint"] = df["endpoint"].str.strip().str.lower()

    removed = initial_count - len(df)
    logger.info(f"Limpeza: {initial_count} → {len(df)} registros ({removed} removidos)")
    return df.reset_index(drop=True)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Enriquecimento: colunas derivadas para análise."""
    # Classificação do status
    df["status_category"] = pd.cut(
        df["status_code"],
        bins=[0, 299, 399, 499, 599],
        labels=["success", "redirect", "client_error", "server_error"],
    )

    # Flag de erro
    df["is_error"] = df["status_code"] >= 400

    # Extrair data e hora
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour

    # Classificação de latência
    df["latency_class"] = pd.cut(
        df["response_time_ms"],
        bins=[0, 200, 500, 1000, float("inf")],
        labels=["fast", "normal", "slow", "critical"],
    )

    logger.info(f"Enriquecimento concluído: {len(df.columns)} colunas")
    return df


def classify_incidents(df: pd.DataFrame) -> pd.DataFrame:
    """Classifica cada registro por severidade de sinistro (regras de negócio).

    Severidades:
    - critical: status 500 + latência > 1000ms (falha grave do servidor sob carga)
    - high: status 500 (erro de servidor) OU latência > 1500ms (degradação severa)
    - medium: status 4xx (erros de cliente) OU latência > 1000ms
    - low: requisições normais sem anomalias
    """
    conditions = [
        (df["status_code"] == 500) & (df["response_time_ms"] > 1000),
        (df["status_code"] == 500) | (df["response_time_ms"] > 1500),
        (df["status_code"] >= 400) | (df["response_time_ms"] > 1000),
    ]
    choices = ["critical", "high", "medium"]
    df["incident_severity"] = pd.Series(
        pd.Categorical(
            values=pd.array(choices)[0:0],  # placeholder
            categories=["low", "medium", "high", "critical"],
            ordered=True,
        )
    )
    df["incident_severity"] = (
        pd.Categorical(
            pd.core.common.np.select(conditions, choices, default="low"),
            categories=["low", "medium", "high", "critical"],
            ordered=True,
        )
    )

    counts = df["incident_severity"].value_counts()
    logger.info(f"Classificação de sinistros: {counts.to_dict()}")
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula indicadores agregados por data para o dashboard operacional."""
    if df.empty:
        logger.warning("DataFrame vazio — indicadores não calculados")
        return pd.DataFrame()

    indicators = df.groupby("date").agg(
        total_requests=("log_id", "count"),
        total_errors=("is_error", "sum"),
        total_critical=("incident_severity", lambda x: (x == "critical").sum()),
        total_high=("incident_severity", lambda x: (x == "high").sum()),
        avg_response_time_ms=("response_time_ms", "mean"),
        p95_response_time_ms=("response_time_ms", lambda x: x.quantile(0.95)),
        error_rate_pct=("is_error", lambda x: round(x.mean() * 100, 2)),
    ).reset_index()

    indicators["avg_response_time_ms"] = indicators["avg_response_time_ms"].round(0).astype("Int64")
    indicators["p95_response_time_ms"] = indicators["p95_response_time_ms"].round(0).astype("Int64")
    indicators["total_errors"] = indicators["total_errors"].astype("Int64")

    logger.info(f"Indicadores calculados: {len(indicators)} dias")
    return indicators


def save_outputs(df: pd.DataFrame, indicators: pd.DataFrame) -> dict:
    """Salva artefatos em CSV e Parquet."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    paths = {}

    # Dados transformados
    csv_path = os.path.join(OUTPUT_DIR, "logs_transformed.csv")
    parquet_path = os.path.join(OUTPUT_DIR, "logs_transformed.parquet")
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    paths["transformed_csv"] = csv_path
    paths["transformed_parquet"] = parquet_path

    # Indicadores
    ind_csv = os.path.join(OUTPUT_DIR, "indicators.csv")
    ind_parquet = os.path.join(OUTPUT_DIR, "indicators.parquet")
    indicators.to_csv(ind_csv, index=False)
    indicators.to_parquet(ind_parquet, index=False, engine="pyarrow")
    paths["indicators_csv"] = ind_csv
    paths["indicators_parquet"] = ind_parquet

    for name, path in paths.items():
        size_kb = os.path.getsize(path) / 1024
        logger.info(f"  {name}: {path} ({size_kb:.1f} KB)")

    return paths


def transform(filepath: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pipeline completo de transformação."""
    logger.info(f"=== INÍCIO DA TRANSFORMAÇÃO: {filepath} ===")

    df = pd.read_csv(filepath)
    df = validate_schema(df)
    df = clean(df)
    df = enrich(df)
    df = classify_incidents(df)
    indicators = compute_indicators(df)
    paths = save_outputs(df, indicators)

    logger.info(f"=== TRANSFORMAÇÃO CONCLUÍDA: {len(df)} registros, {len(indicators)} dias ===")
    return df, indicators
