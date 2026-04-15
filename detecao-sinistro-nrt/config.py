"""Configurações do pipeline."""

PROJECT_ID = "afbcb5ec-ce5d-43a1-b487-d856349f3f02"
API_URL = f"https://api.datamission.com.br/projects/{PROJECT_ID}/dataset?format=csv"
API_TOKEN = "SEU_TOKEN_AQUI"  # Substituir ou usar variável de ambiente DATAMISSION_TOKEN

STAGING_DIR = "staging"
OUTPUT_DIR = "output"
LOGS_DIR = "logs"

# Schema esperado
EXPECTED_COLUMNS = [
    "log_id", "timestamp", "ip_address", "http_method",
    "endpoint", "status_code", "response_time_ms", "user_agent",
]

VALID_HTTP_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
VALID_STATUS_CODES = {200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503}
