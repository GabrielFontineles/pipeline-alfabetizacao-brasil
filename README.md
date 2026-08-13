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
