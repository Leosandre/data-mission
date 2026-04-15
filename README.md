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

**Execução:**
```bash
cd detecao-sinistro-nrt
pip install -r requirements.txt

# Via API
DATAMISSION_TOKEN='seu_token' python3 pipeline.py

# Com dataset local
python3 pipeline.py dataset.csv
```
