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
├── transformacao.py       # Limpeza, validação de schema, enriquecimento
├── validacao.py           # Checks de qualidade dos dados finais
├── pipeline.py            # Orquestrador do pipeline completo
├── requirements.txt       # Dependências Python
├── staging/               # Dados brutos (não versionado)
├── output/                # Dados transformados (não versionado)
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

**Validação:**
```bash
# Verificar se o pipeline gerou os artefatos esperados
ls staging/       # CSV bruto com timestamp
ls output/        # logs_transformed.csv (dados enriquecidos)
cat logs/pipeline.log    # Log do pipeline completo
cat logs/ingestao.log    # Log específico da ingestão (requisições/downloads)

# Validar dados finais manualmente
python3 -c "
import pandas as pd
from validacao import validate
df = pd.read_csv('output/logs_transformed.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])
validate(df)
"
```
