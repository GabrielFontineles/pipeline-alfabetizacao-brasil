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
| RegistrosIngeridos | 35.898 registros totais |
| QualidadeChecksFalhos | 1 (esperado) |
| QualidadeTaxaSucesso | 87.5% |
| StreamingEventosEnviados | 20 |
| StreamingTaxaProcessamento | 100% |

### Alarmes configurados
- **Pipeline-QualidadeDados-Falha**: dispara quando checks de qualidade falham
- **Pipeline-Ingestao-Falha**: dispara quando ingestão batch falha
