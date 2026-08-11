"""
Camada Gold - Dados Analíticos
Lê dados tratados do S3 Silver e gera datasets prontos para análise,
dashboards e modelos de machine learning.
"""

import boto3
import pandas as pd
import io
from datetime import datetime

# Configurações
BUCKET_SILVER = "pipeline-alfabetizacao-silver"
BUCKET_GOLD = "pipeline-alfabetizacao-gold"
REGION = "us-east-1"
DATA_PROCESSAMENTO = "2026-08-11"

def criar_bucket_se_nao_existe(s3_client, bucket_name):
    """Cria o bucket S3 Gold se não existir."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} já existe")
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} criado")

def ler_parquet_s3(s3_client, bucket, key):
    """Lê um arquivo Parquet do S3 e retorna um DataFrame."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)

def salvar_parquet_s3(s3_client, df, bucket, key):
    """Salva um DataFrame como Parquet no S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"  ✓ Salvo em s3://{bucket}/{key}")
    print(f"  Registros: {len(df)} | Colunas: {len(df.columns)}")

def gerar_ranking_estados(df_uf):
    """
    Dataset 1: Ranking de estados por taxa de alfabetização.
    Filtra só rede Total (rede=5) para comparação justa entre estados.
    Inclui distância para a meta 2030.
    """
    print("\n  Gerando ranking de estados...")

    df = df_uf[df_uf["rede"] == "Total"].copy()

    df = df[[
        "ano", "sigla_uf", "taxa_alfabetizacao", "media_portugues",
        "meta_alfabetizacao_2030", "distancia_meta_2030",
        "percentual_participacao"
    ]].copy()

    # Ranking dentro de cada ano
    df["ranking_nacional"] = df.groupby("ano")["taxa_alfabetizacao"].rank(
        ascending=False, method="min"
    ).astype(int)

    # Classifica situação em relação à meta 2030
    df["situacao_meta_2030"] = df["distancia_meta_2030"].apply(
        lambda x: "Meta atingida" if x <= 0
        else "Crítico" if x > 40
        else "Em risco" if x > 20
        else "No caminho"
    )

    df = df.sort_values(["ano", "ranking_nacional"])
    return df

def gerar_evolucao_temporal(df_uf):
    """
    Dataset 2: Evolução de cada estado entre 2023 e 2024.
    Mostra quem avançou, quem regrediu e quanto.
    """
    print("\n  Gerando evolução temporal...")

    df = df_uf[df_uf["rede"] == "Total"].copy()

    df_2023 = df[df["ano"] == 2023][["sigla_uf", "taxa_alfabetizacao"]].rename(
        columns={"taxa_alfabetizacao": "taxa_2023"}
    )
    df_2024 = df[df["ano"] == 2024][["sigla_uf", "taxa_alfabetizacao", "meta_alfabetizacao_2030"]].rename(
        columns={"taxa_alfabetizacao": "taxa_2024"}
    )

    df_evolucao = pd.merge(df_2023, df_2024, on="sigla_uf", how="inner")

    # Variação absoluta e percentual
    df_evolucao["variacao_pontos"] = df_evolucao["taxa_2024"] - df_evolucao["taxa_2023"]
    df_evolucao["variacao_percentual"] = (
        (df_evolucao["taxa_2024"] - df_evolucao["taxa_2023"]) / df_evolucao["taxa_2023"] * 100
    ).round(2)

    # Classifica tendência
    df_evolucao["tendencia"] = df_evolucao["variacao_pontos"].apply(
        lambda x: "Avançou" if x > 0 else "Regrediu" if x < 0 else "Estável"
    )

    df_evolucao["distancia_meta_2030"] = (
        df_evolucao["meta_alfabetizacao_2030"] - df_evolucao["taxa_2024"]
    )

    df_evolucao = df_evolucao.sort_values("variacao_pontos", ascending=False)
    return df_evolucao

def gerar_analise_municipal(df_municipio):
    """
    Dataset 3: Análise municipal agregada por estado.
    Mostra média, mínimo, máximo e municípios críticos por UF.
    """
    print("\n  Gerando análise municipal...")

    # Filtra só rede Total e ano mais recente
    df = df_municipio[
        (df_municipio["rede"] == "Total") &
        (df_municipio["ano"] == 2024)
    ].copy()

    # Extrai UF do código do município (primeiros 2 dígitos do IBGE)
    df["cod_uf"] = df["id_municipio"].str[:2]

    # Agrega por UF
    df_agregado = df.groupby("cod_uf").agg(
        total_municipios=("id_municipio", "count"),
        taxa_media=("taxa_alfabetizacao", "mean"),
        taxa_minima=("taxa_alfabetizacao", "min"),
        taxa_maxima=("taxa_alfabetizacao", "max"),
        municipios_criticos=("taxa_alfabetizacao", lambda x: (x < 40).sum()),
        municipios_meta_atingida=("distancia_meta_2030", lambda x: (x <= 0).sum())
    ).reset_index()

    df_agregado["taxa_media"] = df_agregado["taxa_media"].round(2)
    df_agregado["pct_municipios_criticos"] = (
        df_agregado["municipios_criticos"] / df_agregado["total_municipios"] * 100
    ).round(2)

    df_agregado = df_agregado.sort_values("taxa_media")
    return df_agregado

def main():
    print("=" * 50)
    print("GERAÇÃO GOLD LAYER")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    s3_client = boto3.client("s3", region_name=REGION)
    criar_bucket_se_nao_existe(s3_client, BUCKET_GOLD)

    data_geracao = datetime.now().strftime("%Y-%m-%d")

    # --- Leitura do Silver ---
    print("\n[1/3] Lendo dados do Silver...")
    df_uf = ler_parquet_s3(s3_client, BUCKET_SILVER,
        f"silver/uf/processamento_date={DATA_PROCESSAMENTO}/uf_silver.parquet")
    df_municipio = ler_parquet_s3(s3_client, BUCKET_SILVER,
        f"silver/municipio/processamento_date={DATA_PROCESSAMENTO}/municipio_silver.parquet")
    print("✓ Dados carregados do Silver")

    # --- Geração dos datasets Gold ---
    print("\n[2/3] Gerando datasets analíticos...")
    df_ranking = gerar_ranking_estados(df_uf)
    df_evolucao = gerar_evolucao_temporal(df_uf)
    df_municipal = gerar_analise_municipal(df_municipio)

    # --- Salvar no Gold ---
    print("\n[3/3] Salvando no S3 Gold...")
    salvar_parquet_s3(s3_client, df_ranking, BUCKET_GOLD,
        f"gold/ranking_estados/geracao_date={data_geracao}/ranking_estados.parquet")
    salvar_parquet_s3(s3_client, df_evolucao, BUCKET_GOLD,
        f"gold/evolucao_temporal/geracao_date={data_geracao}/evolucao_temporal.parquet")
    salvar_parquet_s3(s3_client, df_municipal, BUCKET_GOLD,
        f"gold/analise_municipal/geracao_date={data_geracao}/analise_municipal.parquet")

    # --- Preview dos resultados ---
    print("\n📊 PREVIEW — Ranking de Estados (2024):")
    preview = df_ranking[df_ranking["ano"] == 2024][
        ["ranking_nacional", "sigla_uf", "taxa_alfabetizacao", "situacao_meta_2030"]
    ].head(10)
    print(preview.to_string(index=False))

    print("\n📊 PREVIEW — Evolução 2023→2024 (Top 5 avanços):")
    print(df_evolucao[["sigla_uf", "taxa_2023", "taxa_2024", "variacao_pontos", "tendencia"]].head(5).to_string(index=False))

    print(f"\n{'=' * 50}")
    print("Gold Layer concluída com sucesso!")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
