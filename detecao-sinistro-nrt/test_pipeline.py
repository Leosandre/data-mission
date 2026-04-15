"""Testes unitários do pipeline de detecção de sinistros."""

import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from config import EXPECTED_COLUMNS, VALID_HTTP_METHODS, VALID_STATUS_CODES
from transformacao import validate_schema, clean, enrich, classify_incidents, compute_indicators, save_outputs, transform
from validacao import validate


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df():
    """DataFrame válido com registros variados."""
    return pd.DataFrame({
        "log_id": ["aaa-111", "bbb-222", "ccc-333", "ddd-444", "eee-555"],
        "timestamp": [
            "2026-04-15T10:00:00", "2026-04-15T10:01:00",
            "2026-04-15T10:02:00", "2026-04-15T10:03:00", "2026-04-15T10:04:00",
        ],
        "ip_address": ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5"],
        "http_method": ["GET", "POST", "PUT", "DELETE", "GET"],
        "endpoint": ["api/health", "api/login", "api/data", "api/old", "api/search"],
        "status_code": [200, 201, 500, 404, 400],
        "response_time_ms": [50, 300, 1500, 800, 1200],
        "user_agent": ["Mozilla/5.0"] * 5,
    })


@pytest.fixture
def empty_csv():
    """CSV com header mas sem dados."""
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    tmp.write(",".join(EXPECTED_COLUMNS) + "\n")
    tmp.close()
    yield tmp.name
    os.unlink(tmp.name)


@pytest.fixture
def valid_csv(sample_df):
    """CSV com dados válidos."""
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    sample_df.to_csv(tmp.name, index=False)
    yield tmp.name
    os.unlink(tmp.name)


# ── validate_schema ───────────────────────────────────────────────────────────

class TestValidateSchema:
    def test_valid_columns(self, sample_df):
        result = validate_schema(sample_df)
        assert list(result.columns) == EXPECTED_COLUMNS

    def test_missing_column_raises(self, sample_df):
        df = sample_df.drop(columns=["status_code"])
        with pytest.raises(ValueError, match="Colunas ausentes"):
            validate_schema(df)

    def test_extra_columns_ignored(self, sample_df):
        sample_df["extra_col"] = "x"
        result = validate_schema(sample_df)
        assert "extra_col" not in result.columns


# ── clean ─────────────────────────────────────────────────────────────────────

class TestClean:
    def test_removes_duplicates(self, sample_df):
        df = pd.concat([sample_df, sample_df.iloc[[0]]])
        result = clean(df)
        assert result["log_id"].is_unique

    def test_removes_null_required_fields(self, sample_df):
        sample_df.loc[0, "log_id"] = None
        result = clean(sample_df)
        assert len(result) == 4

    def test_removes_invalid_http_method(self, sample_df):
        sample_df.loc[0, "http_method"] = "INVALID"
        result = clean(sample_df)
        assert "INVALID" not in result["http_method"].values

    def test_removes_invalid_status_code(self, sample_df):
        sample_df.loc[0, "status_code"] = 999
        result = clean(sample_df)
        assert 999 not in result["status_code"].values

    def test_normalizes_endpoint(self, sample_df):
        sample_df.loc[0, "endpoint"] = "  API/Health  "
        result = clean(sample_df)
        assert result.loc[0, "endpoint"] == "api/health"

    def test_invalid_timestamp_removed(self, sample_df):
        sample_df.loc[0, "timestamp"] = "not-a-date"
        result = clean(sample_df)
        assert len(result) == 4

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame(columns=EXPECTED_COLUMNS)
        result = clean(df)
        assert len(result) == 0


# ── enrich ────────────────────────────────────────────────────────────────────

class TestEnrich:
    def test_adds_expected_columns(self, sample_df):
        df = clean(sample_df)
        result = enrich(df)
        for col in ["status_category", "is_error", "date", "hour", "latency_class"]:
            assert col in result.columns

    def test_is_error_flag(self, sample_df):
        df = enrich(clean(sample_df))
        assert df.loc[df["status_code"] == 200, "is_error"].iloc[0] == False
        assert df.loc[df["status_code"] == 500, "is_error"].iloc[0] == True

    def test_status_category(self, sample_df):
        df = enrich(clean(sample_df))
        assert df.loc[df["status_code"] == 200, "status_category"].iloc[0] == "success"
        assert df.loc[df["status_code"] == 500, "status_category"].iloc[0] == "server_error"
        assert df.loc[df["status_code"] == 404, "status_category"].iloc[0] == "client_error"

    def test_latency_class(self, sample_df):
        df = enrich(clean(sample_df))
        row_fast = df.loc[df["response_time_ms"] == 50]
        row_critical = df.loc[df["response_time_ms"] == 1500]
        assert row_fast["latency_class"].iloc[0] == "fast"
        assert row_critical["latency_class"].iloc[0] == "critical"


# ── classify_incidents ────────────────────────────────────────────────────────

class TestClassifyIncidents:
    def test_critical(self, sample_df):
        """status 500 + response_time > 1000ms = critical."""
        df = classify_incidents(enrich(clean(sample_df)))
        row = df.loc[(df["status_code"] == 500) & (df["response_time_ms"] > 1000)]
        assert row["incident_severity"].iloc[0] == "critical"

    def test_high(self, sample_df):
        """status 500 com latência baixa = high."""
        sample_df.loc[2, "response_time_ms"] = 100  # 500 + low latency
        df = classify_incidents(enrich(clean(sample_df)))
        row = df.loc[df["status_code"] == 500]
        assert row["incident_severity"].iloc[0] == "high"

    def test_medium(self, sample_df):
        """status 4xx = medium."""
        df = classify_incidents(enrich(clean(sample_df)))
        row = df.loc[df["status_code"] == 404]
        assert row["incident_severity"].iloc[0] == "medium"

    def test_low(self, sample_df):
        """status 200 + latência baixa = low."""
        df = classify_incidents(enrich(clean(sample_df)))
        row = df.loc[(df["status_code"] == 200) & (df["response_time_ms"] <= 1000)]
        assert row["incident_severity"].iloc[0] == "low"

    def test_severity_categories(self, sample_df):
        df = classify_incidents(enrich(clean(sample_df)))
        assert set(df["incident_severity"].cat.categories) == {"low", "medium", "high", "critical"}


# ── compute_indicators ────────────────────────────────────────────────────────

class TestComputeIndicators:
    def test_indicators_columns(self, sample_df):
        df = classify_incidents(enrich(clean(sample_df)))
        ind = compute_indicators(df)
        for col in ["total_requests", "total_errors", "total_critical", "total_high",
                     "avg_response_time_ms", "p95_response_time_ms", "error_rate_pct"]:
            assert col in ind.columns

    def test_indicators_values(self, sample_df):
        df = classify_incidents(enrich(clean(sample_df)))
        ind = compute_indicators(df)
        assert ind["total_requests"].iloc[0] == 5
        assert ind["total_errors"].iloc[0] == 3  # 500, 404, 400

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame(columns=EXPECTED_COLUMNS + ["is_error", "incident_severity", "date", "response_time_ms"])
        result = compute_indicators(df)
        assert len(result) == 0


# ── save_outputs ──────────────────────────────────────────────────────────────

class TestSaveOutputs:
    def test_creates_all_files(self, sample_df, tmp_path):
        df = classify_incidents(enrich(clean(sample_df)))
        ind = compute_indicators(df)
        import config
        original = config.OUTPUT_DIR
        config.OUTPUT_DIR = str(tmp_path)
        try:
            from transformacao import save_outputs as _save
            # Patch OUTPUT_DIR in transformacao module
            import transformacao
            transformacao.OUTPUT_DIR = str(tmp_path)
            paths = transformacao.save_outputs(df, ind)
            for path in paths.values():
                assert os.path.exists(path)
                assert os.path.getsize(path) > 0
        finally:
            config.OUTPUT_DIR = original
            transformacao.OUTPUT_DIR = original

    def test_parquet_readable(self, sample_df, tmp_path):
        df = classify_incidents(enrich(clean(sample_df)))
        ind = compute_indicators(df)
        import transformacao
        original = transformacao.OUTPUT_DIR
        transformacao.OUTPUT_DIR = str(tmp_path)
        try:
            paths = transformacao.save_outputs(df, ind)
            df_read = pd.read_parquet(paths["transformed_parquet"])
            assert len(df_read) == len(df)
            ind_read = pd.read_parquet(paths["indicators_parquet"])
            assert len(ind_read) == len(ind)
        finally:
            transformacao.OUTPUT_DIR = original


# ── validate (validacao.py) ───────────────────────────────────────────────────

class TestValidate:
    def test_passed_with_valid_data(self, sample_df):
        df = classify_incidents(enrich(clean(sample_df)))
        ind = compute_indicators(df)
        report = validate(df, ind)
        assert report["validation_passed"] == True
        assert report["log_id_duplicados"] == 0
        assert report["total_registros"] == 5

    def test_failed_with_empty_data(self):
        df = pd.DataFrame(columns=EXPECTED_COLUMNS + [
            "status_category", "is_error", "date", "hour", "latency_class", "incident_severity",
        ])
        report = validate(df)
        assert report["validation_passed"] == False
        assert report["latency_p50_ms"] is None

    def test_incident_distribution_present(self, sample_df):
        df = classify_incidents(enrich(clean(sample_df)))
        report = validate(df)
        assert "incident_distribution" in report

    def test_indicator_days_present(self, sample_df):
        df = classify_incidents(enrich(clean(sample_df)))
        ind = compute_indicators(df)
        report = validate(df, ind)
        assert report["indicator_days"] == 1  # all same date


# ── transform (end-to-end) ────────────────────────────────────────────────────

class TestTransformEndToEnd:
    def test_full_pipeline_with_csv(self, valid_csv):
        df, ind = transform(valid_csv)
        assert len(df) == 5
        assert "incident_severity" in df.columns
        assert len(ind) > 0

    def test_empty_csv(self, empty_csv):
        df, ind = transform(empty_csv)
        assert len(df) == 0
        assert len(ind) == 0
