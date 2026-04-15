# Data Mission

Repositório de projetos da plataforma [DataMission](https://datamission.com.br/).

## Projetos

### 🏥 Detecção de Sinistros NRT — Hospital Costela

Pipeline de ingestão, transformação e validação de dados de logs para detecção de anomalias em tempo quase real.

**Estrutura:**
```
detecao-sinistro-nrt/
├── config.py              # Configurações (API, schema, validações)
├── ingestao.py            # Coleta de dados via API DataMission
├── transformacao.py       # Limpeza, validação de schema, enriquecimento, regras de negócio
├── validacao.py           # Checks de qualidade dos dados finais
├── pipeline.py            # Orquestrador do pipeline completo
├── test_pipeline.py       # Testes unitários (30 testes)
├── requirements.txt       # Dependências Python
├── staging/               # Dados brutos (não versionado)
├── output/                # Dados transformados (não versionado)
│   ├── logs_transformed.csv / .parquet   # Registros enriquecidos com classificação
│   └── indicators.csv / .parquet         # Indicadores diários agregados
└── logs/                  # Logs de execução (não versionado)
    ├── pipeline.log       # Log do pipeline completo
    └── ingestao.log       # Log específico da ingestão (requisições/downloads)
```

---

### Configuração

O token de acesso à API **nunca deve ser commitado no código**. Use a variável de ambiente `DATAMISSION_TOKEN`:

```bash
export DATAMISSION_TOKEN='seu_token_aqui'
```

> Gere sua chave na aba **Projetos Ativos** (botão GERAR API KEY) em [datamission.com.br](https://datamission.com.br/).

---

### Execução

```bash
cd detecao-sinistro-nrt
pip install -r requirements.txt

# Via API (requer DATAMISSION_TOKEN configurado)
python3 pipeline.py

# Com dataset local
python3 pipeline.py dataset.csv

# Módulo de ingestão isolado
python3 ingestao.py
```

**Parâmetros:**

| Parâmetro | Descrição |
|---|---|
| `DATAMISSION_TOKEN` (env var) | Token de autenticação da API DataMission |
| `argv[1]` (opcional) | Caminho para CSV local — se omitido, busca da API |

---

### Pipeline — etapas

1. **Ingestão** (`ingestao.py`): GET na API → salva CSV bruto em `staging/` com timestamp
2. **Transformação** (`transformacao.py`):
   - Validação de schema (colunas esperadas)
   - Limpeza (tipos, nulos, duplicatas, métodos HTTP e status codes inválidos)
   - Enriquecimento (status_category, is_error, latency_class, date, hour)
   - Classificação de sinistros por severidade (critical/high/medium/low)
   - Cálculo de indicadores diários (requests, erros, latência p95, taxa de erro)
   - Exportação em CSV e Parquet
3. **Validação** (`validacao.py`): Completude, unicidade, volume, distribuição, latência, sinistros

---

### Regras de negócio — classificação de sinistros

| Severidade | Regra |
|---|---|
| `critical` | Status 500 + latência > 1000ms |
| `high` | Status 500 OU latência > 1500ms |
| `medium` | Status 4xx OU latência > 1000ms |
| `low` | Requisições normais |

---

### Indicadores diários (`output/indicators.parquet`)

| Indicador | Descrição |
|---|---|
| `total_requests` | Total de requisições no dia |
| `total_errors` | Requisições com status >= 400 |
| `total_critical` | Sinistros classificados como critical |
| `total_high` | Sinistros classificados como high |
| `avg_response_time_ms` | Tempo médio de resposta |
| `p95_response_time_ms` | Percentil 95 de latência |
| `error_rate_pct` | Taxa de erro (%) |

---

### Testes

```bash
# Rodar todos os testes unitários (30 testes)
python3 -m pytest test_pipeline.py -v
```

**Cobertura dos testes:**

| Módulo | Testes | O que valida |
|---|---|---|
| `validate_schema` | 3 | Colunas válidas, ausentes, extras |
| `clean` | 7 | Duplicatas, nulos, HTTP inválido, status inválido, timestamp, endpoint, vazio |
| `enrich` | 4 | Colunas derivadas, flags, categorias, latência |
| `classify_incidents` | 5 | Cada severidade, categorias ordenadas |
| `compute_indicators` | 3 | Colunas, valores, DataFrame vazio |
| `save_outputs` | 2 | Criação de arquivos, Parquet legível |
| `validate` | 4 | Dados válidos, vazios, distribuição, indicadores |
| `transform` (e2e) | 2 | Pipeline completo com CSV, CSV vazio |

---

### Pontos de falha esperados

| Cenário | Comportamento |
|---|---|
| Token inválido/ausente | `ValueError` antes da chamada à API |
| API fora do ar / timeout | `HTTPError` / `ConnectionError` com log do erro |
| CSV com colunas faltando | `ValueError("Colunas ausentes")` na validação de schema |
| Todos os registros inválidos | Pipeline conclui com `validation_passed: False` (sem exceção) |
| DataFrame vazio | Indicadores vazios, latências como `None`, validação falha graciosamente |
| Diretórios inexistentes | Criados automaticamente (`os.makedirs`) |

---

### Validação manual

```bash
# Verificar artefatos gerados
ls output/

# Validar dados finais
python3 -c "
import pandas as pd
from validacao import validate
df = pd.read_parquet('output/logs_transformed.parquet')
validate(df, pd.read_parquet('output/indicators.parquet'))
"

# Verificar logs
cat logs/pipeline.log
cat logs/ingestao.log
```
