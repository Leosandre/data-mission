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
├── requirements.txt       # Dependências Python
├── staging/               # Dados brutos (não versionado)
├── output/                # Dados transformados (não versionado)
│   ├── logs_transformed.csv / .parquet   # Registros enriquecidos com classificação
│   └── indicators.csv / .parquet         # Indicadores diários agregados
└── logs/                  # Logs de execução (não versionado)
```

**Configuração:**

O token de acesso à API **nunca deve ser commitado no código**. Use a variável de ambiente `DATAMISSION_TOKEN`:

```bash
export DATAMISSION_TOKEN='seu_token_aqui'
```

> Gere sua chave na aba **Projetos Ativos** (botão GERAR API KEY) em [datamission.com.br](https://datamission.com.br/).

**Execução:**
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

**Pipeline — etapas:**

1. **Ingestão** (`ingestao.py`): GET na API → salva CSV bruto em `staging/` com timestamp
2. **Transformação** (`transformacao.py`):
   - Validação de schema (colunas esperadas)
   - Limpeza (tipos, nulos, duplicatas, métodos HTTP inválidos)
   - Enriquecimento (status_category, is_error, latency_class, date, hour)
   - Classificação de sinistros por severidade (critical/high/medium/low)
   - Cálculo de indicadores diários (requests, erros, latência p95, taxa de erro)
   - Saída em CSV e Parquet
3. **Validação** (`validacao.py`): Completude, unicidade, volume, distribuição, latência

**Regras de negócio — classificação de sinistros:**

| Severidade | Regra |
|---|---|
| `critical` | Status 500 + latência > 1000ms |
| `high` | Status 500 OU latência > 1500ms |
| `medium` | Status 4xx OU latência > 1000ms |
| `low` | Requisições normais |

**Indicadores diários** (`output/indicators.parquet`):

| Indicador | Descrição |
|---|---|
| `total_requests` | Total de requisições no dia |
| `total_errors` | Requisições com status >= 400 |
| `total_critical` | Sinistros classificados como critical |
| `total_high` | Sinistros classificados como high |
| `avg_response_time_ms` | Tempo médio de resposta |
| `p95_response_time_ms` | Percentil 95 de latência |
| `error_rate_pct` | Taxa de erro (%) |

**Validação:**
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
