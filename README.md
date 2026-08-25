# Pipeline Híbrido para Análise da Alfabetização no Brasil

## Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do Brasil. O **Compromisso Nacional Criança Alfabetizada** estabelece como meta que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental até 2030.

Para monitorar esse objetivo, o INEP criou o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem 743 pontos na escala de proficiência do SAEB — o ponto de corte definido pela Pesquisa Alfabetiza Brasil (2023).

Este projeto constrói uma pipeline híbrida de dados capaz de integrar, tratar e disponibilizar esses dados para análises educacionais em escala nacional.

---

## Fontes de Dados

Todos os dados são provenientes da plataforma **Base dos Dados** (`basedosdados.org`), dataset `br-inep-avaliacao-alfabetizacao`:

| Tabela | Registros | Descrição |
|---|---|---|
| `uf` | 145 | Taxa de alfabetização por estado e rede |
| `meta_alfabetizacao_uf` | 54 | Metas 2024-2030 por estado |
| `municipio` | 23.995 | Taxa de alfabetização por município |
| `meta_alfabetizacao_municipio` | 10.704 | Metas 2024-2030 por município |
| `aluno` | ~256MB | Microdados individuais (acesso via BigQuery) |

---

## Arquitetura da Solução

Fonte: Base dos Dados (basedosdados.org)
│
▼
┌─────────────────────────────────────────────┐
│ INGESTÃO BATCH │
│ Python + boto3 → S3 Bronze (Parquet) │
└─────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────┐
│ BRONZE LAYER (S3) │
│ Dados brutos particionados por data │
└─────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────┐
│ SILVER LAYER (S3) │
│ Limpeza, padronização e integração │
└─────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────┐
│ GOLD LAYER (S3) │
│ Datasets analíticos prontos para uso │
└─────────────────────────────────────────────┘

STREAMING (paralelo):
Script Python → SQS → Lambda → S3 Bronze/streaming/


---

## Stack Tecnológico

| Componente | Tecnologia | Justificativa |
|---|---|---|
| Cloud | AWS | Disponibilidade, maturidade e ecossistema |
| Storage | Amazon S3 | Serverless, durável, custo baixo |
| Processamento | Python + boto3 | Flexibilidade e integração nativa com AWS |
| Streaming | SQS + Lambda | Serverless, sem custo fixo, escalável |
| Formato | Parquet | 70% menor que CSV, leitura colunar eficiente |
| Monitoramento | CloudWatch | Nativo AWS, sem infraestrutura adicional |
| Versionamento | Git + GitHub | Rastreabilidade e colaboração |

---

## Arquitetura Medalhão

### Bronze Layer — Dados Brutos
- Dados ingeridos sem transformações
- Formato Parquet particionado por `ingestao_date`
- Histórico completo preservado
- Bucket: `pipeline-alfabetizacao-bronze`

### Silver Layer — Dados Tratados
- Limpeza de valores nulos
- Padronização de tipos e categorias
- Integração entre tabelas (resultado real + metas)
- Nova coluna `distancia_meta_2030` calculada
- Bucket: `pipeline-alfabetizacao-silver`

### Gold Layer — Camada Analítica
- **ranking_estados**: ranking nacional por taxa de alfabetização com situação em relação à meta 2030
- **evolucao_temporal**: variação 2023→2024 por estado com tendência
- **analise_municipal**: agregação municipal por UF com percentual de municípios críticos
- Bucket: `pipeline-alfabetizacao-gold`

---

## Pipeline de Streaming

Simula a chegada de novos resultados de avaliações em tempo quase real:

simulate_stream.py → SQS (pipeline-alfabetizacao-stream)
→ Lambda (pipeline-alfabetizacao-stream-processor)
→ S3 Bronze/streaming/ano={ano}/uf={uf}/


### Decisão arquitetural — SQS vs Kinesis
O SQS foi escolhido por ser mais simples e econômico para o volume simulado. O Kinesis seria necessário apenas para milhões de eventos por segundo — fora do escopo deste projeto.

### Características
- Eventos particionados por ano e UF
- Lambda com trigger automático no SQS
- Processamento em lotes de até 5 mensagens
- 20 eventos simulados processados com 100% de sucesso

---

## Qualidade de Dados

O script `quality/data_quality.py` executa 8 checks automatizados:

| Check | Resultado |
|---|---|
| Completude — bronze/uf | ✅ 0 nulos |
| Duplicidade — bronze/uf | ✅ 0 duplicatas |
| Consistência — bronze/uf | ✅ 0 violações |
| Completude — bronze/municipio | ✅ 0 nulos |
| Duplicidade — bronze/municipio | ✅ 0 duplicatas |
| Completude — silver/uf | ✅ 0 nulos |
| Completude — silver/municipio | ✅ 0 nulos |
| Integridade — municipio → meta | ⚠️ 198 municípios sem meta |

> **Nota**: Os 198 municípios sem correspondência de meta é uma característica real dos dados — municípios com participação insuficiente não recebem metas individualizadas pelo programa nacional. Não representa erro no pipeline.

---

## Monitoramento

Métricas publicadas no CloudWatch sob o namespace `PipelineAlfabetizacao`:

| Métrica | Valor |
|---|---|
| RegistrosIngeridos | 34.898 registros totais |
| QualidadeChecksFalhos | 1 (esperado) |
| QualidadeTaxaSucesso | 87.5% |
| StreamingEventosEnviados | 20 |
| StreamingTaxaProcessamento | 100% |

### Alarmes configurados
- **Pipeline-QualidadeDados-Falha**: dispara quando checks de qualidade falham
- **Pipeline-Ingestao-Falha**: dispara quando ingestão batch falha

---

## FinOps — Otimização de Custos

| Decisão | Impacto |
|---|---|
| Parquet em vez de CSV | Redução de ~70% no armazenamento |
| Serverless (Lambda, SQS) | Zero custo fixo — paga só pelo uso |
| Sem EC2 | Elimina custo de instâncias paradas |
| Particionamento por data/UF | Queries mais baratas e rápidas |
| Sem Glue/EMR | Evita custo de clusters gerenciados |

### Custo real do projeto
| Serviço | Custo estimado |
|---|---|
| S3 (3 buckets) | ~$0.50 |
| Lambda (execuções) | ~$0.00 |
| SQS (20 mensagens) | ~$0.00 |
| CloudWatch (métricas) | ~$0.50 |
| **Total** | **$0 de $50 disponíveis** |

> O projeto foi concluído com **custo zero** dentro do orçamento do AWS Academy Learner Lab, demonstrando que arquiteturas serverless bem planejadas eliminam desperdício de recursos.

---

## Potencial para Inteligência Artificial

A camada Gold está preparada para alimentar modelos de ML e análises avançadas:

### Modelos preditivos
- **Predição de alfabetização**: usar dados históricos de UF e município para prever taxa futura e antecipar municípios em risco
- **Detecção de anomalias**: identificar quedas abruptas de desempenho — como a queda de 19 pontos do RS entre 2023 e 2024

### Análise de desigualdade
- **Clustering de vulnerabilidade**: agrupar municípios por perfil educacional para direcionar políticas públicas com mais precisão
- **Gap regional**: o Ceará (85%) versus estados mais vulneráveis (~35%) representa uma desigualdade de 50 pontos percentuais que pode ser modelada e explicada

### Políticas públicas baseadas em dados
- Identificar quais fatores socioeconômicos mais influenciam a taxa de alfabetização
- Simular o impacto de intervenções educacionais por região
- Priorizar alocação de recursos nos 198 municípios sem meta definida

---

## Como Executar

### Pré-requisitos
- Python 3.12+
- Conta AWS com credenciais configuradas
- Arquivos CSV da Base dos Dados em `data/raw/`

### Instalação
```bash
pip install boto3 pandas pyarrow awscli
```

### Execução da pipeline completa
```bash
# 1. Ingestão Batch → Bronze Layer
python pipeline/ingestion/batch/ingest_batch.py

# 2. Transformação → Silver Layer
python pipeline/silver/silver_layer.py

# 3. Agregação → Gold Layer
python pipeline/gold/gold_layer.py

# 4. Streaming simulado
python pipeline/ingestion/streaming/simulate_stream.py

# 5. Deploy da Lambda (primeira vez apenas)
python pipeline/ingestion/streaming/deploy_lambda.py

# 6. Validação de qualidade
python quality/data_quality.py

# 7. Monitoramento CloudWatch
python monitoring/cloudwatch_alerts.py
```

---

## Estrutura do Repositório

pipeline-alfabetizacao-brasil/
├── pipeline/
│ ├── ingestion/
│ │ ├── batch/
│ │ │ └── ingest_batch.py
│ │ └── streaming/
│ │ ├── simulate_stream.py
│ │ ├── lambda_processor.py
│ │ └── deploy_lambda.py
│ ├── silver/
│ │ └── silver_layer.py
│ └── gold/
│ └── gold_layer.py
├── quality/
│ └── data_quality.py
├── monitoring/
│ └── cloudwatch_alerts.py
├── data/
│ └── raw/
└── README.md


---

## Equipe

Projeto desenvolvido como Tech Challenge — Fase 2
Pós-Tech FIAP — Engenharia de Dados
