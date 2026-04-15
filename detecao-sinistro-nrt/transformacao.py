"""Módulo de transformação — validação de schema, limpeza e enriquecimento."""

import logging
import os

import pandas as pd

from config import EXPECTED_COLUMNS, VALID_HTTP_METHODS, VALID_STATUS_CODES, OUTPUT_DIR

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


def transform(filepath: str) -> pd.DataFrame:
    """Pipeline completo de transformação."""
    logger.info(f"=== INÍCIO DA TRANSFORMAÇÃO: {filepath} ===")

    df = pd.read_csv(filepath)
    df = validate_schema(df)
    df = clean(df)
    df = enrich(df)

    # Salvar resultado
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "logs_transformed.csv")
    df.to_csv(output_path, index=False)
    logger.info(f"=== TRANSFORMAÇÃO CONCLUÍDA: {output_path} ({len(df)} registros) ===")

    return df
