"""
Camada Gold - Dados Analíticos
Lê dados tratados do S3 Silver e gera datasets prontos para análise.
Partição descoberta dinamicamente — não depende de data fixa.
"""

import boto3
import pandas as pd
import io
from datetime import datetime

BUCKET_SILVER = "pipeline-alfabetizacao-silver"
BUCKET_GOLD = "pipeline-alfabetizacao-gold"
REGION = "us-east-1"

def criar_bucket_se_nao_existe(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} já existe")
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} criado")

def descobrir_particao_recente(s3_client, bucket, prefixo):
    """Descobre dinamicamente a partição mais recente no S3."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo, Delimiter='/')
    if 'CommonPrefixes' not in response:
        raise ValueError(f"Nenhuma partição encontrada em s3://{bucket}/{prefixo}")
    particoes = [p['Prefix'] for p in response['CommonPrefixes']]
    particao = sorted(particoes)[-1]
    print(f"  Partição encontrada: {particao}")
    return particao

def ler_parquet_s3(s3_client, bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)

def salvar_parquet_s3(s3_client, df, bucket, key):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"  ✓ Salvo em s3://{bucket}/{key}")
    print(f"  Registros: {len(df)} | Colunas: {len(df.columns)}")

def gerar_ranking_estados(df_uf):
    print("\n  Gerando ranking de estados...")
    df = df_uf[df_uf["rede"] == "Total"].copy()
    df = df[[
        "ano", "sigla_uf", "taxa_alfabetizacao", "media_portugues",
        "meta_alfabetizacao_2030", "distancia_meta_2030",
        "percentual_participacao"
    ]].copy()
    df["ranking_nacional"] = df.groupby("ano")["taxa_alfabetizacao"].rank(
        ascending=False, method="min"
    ).astype(int)
    df["situacao_meta_2030"] = df["distancia_meta_2030"].apply(
        lambda x: "Meta atingida" if pd.notna(x) and x <= 0
        else "Crítico" if pd.notna(x) and x > 40
        else "Em risco" if pd.notna(x) and x > 20
        else "No caminho" if pd.notna(x)
        else "Sem meta definida"
    )
    df = df.sort_values(["ano", "ranking_nacional"])
    return df

def gerar_evolucao_temporal(df_uf):
    print("\n  Gerando evolução temporal...")
    df = df_uf[df_uf["rede"] == "Total"].copy()
    df_2023 = df[df["ano"] == 2023][["sigla_uf", "taxa_alfabetizacao"]].rename(
        columns={"taxa_alfabetizacao": "taxa_2023"}
    )
    df_2024 = df[df["ano"] == 2024][["sigla_uf", "taxa_alfabetizacao", "meta_alfabetizacao_2030"]].rename(
        columns={"taxa_alfabetizacao": "taxa_2024"}
    )
    df_evolucao = pd.merge(df_2023, df_2024, on="sigla_uf", how="inner")
    df_evolucao["variacao_pontos"] = df_evolucao["taxa_2024"] - df_evolucao["taxa_2023"]
    df_evolucao["variacao_percentual"] = (
        (df_evolucao["taxa_2024"] - df_evolucao["taxa_2023"]) / df_evolucao["taxa_2023"] * 100
    ).round(2)
    df_evolucao["tendencia"] = df_evolucao["variacao_pontos"].apply(
        lambda x: "Avançou" if x > 0 else "Regrediu" if x < 0 else "Estável"
    )
    df_evolucao["distancia_meta_2030"] = (
        df_evolucao["meta_alfabetizacao_2030"] - df_evolucao["taxa_2024"]
    )
    df_evolucao = df_evolucao.sort_values("variacao_pontos", ascending=False)
    return df_evolucao

def gerar_analise_municipal(df_municipio):
    print("\n  Gerando análise municipal...")
    df = df_municipio[
        (df_municipio["rede"] == "Total") &
        (df_municipio["ano"] == 2024)
    ].copy()
    df["cod_uf"] = df["id_municipio"].str[:2]
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

def gerar_visao_brasil(df_uf):
    """
    Dataset 4: Visão nacional agregada por ano.
    Cobre a entidade Meta Alfabetização Brasil por derivação —
    não existe como tabela separada na Base dos Dados.
    """
    print("\n  Gerando visão Brasil...")
    df = df_uf[df_uf["rede"] == "Total"].copy()
    df_brasil = df.groupby("ano").agg(
        taxa_media_nacional=("taxa_alfabetizacao", "mean"),
        taxa_mediana_nacional=("taxa_alfabetizacao", "median"),
        melhor_taxa=("taxa_alfabetizacao", "max"),
        pior_taxa=("taxa_alfabetizacao", "min"),
        amplitude=("taxa_alfabetizacao", lambda x: x.max() - x.min()),
        estados_avaliados=("sigla_uf", "count"),
        distancia_media_meta_2030=("distancia_meta_2030", "mean")
    ).reset_index()
    df_brasil["taxa_media_nacional"] = df_brasil["taxa_media_nacional"].round(2)
    df_brasil["taxa_mediana_nacional"] = df_brasil["taxa_mediana_nacional"].round(2)
    df_brasil["distancia_media_meta_2030"] = df_brasil["distancia_media_meta_2030"].round(2)
    return df_brasil

def main():
    print("=" * 50)
    print("GERAÇÃO GOLD LAYER")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    s3_client = boto3.client("s3", region_name=REGION)
    criar_bucket_se_nao_existe(s3_client, BUCKET_GOLD)
    data_geracao = datetime.now().strftime("%Y-%m-%d")

    print("\n[1/3] Descobrindo partições no Silver...")
    part_uf = descobrir_particao_recente(s3_client, BUCKET_SILVER, "silver/uf/")
    part_municipio = descobrir_particao_recente(s3_client, BUCKET_SILVER, "silver/municipio/")

    print("\n[2/3] Lendo dados do Silver...")
    df_uf = ler_parquet_s3(s3_client, BUCKET_SILVER, f"{part_uf}uf_silver.parquet")
    df_municipio = ler_parquet_s3(s3_client, BUCKET_SILVER, f"{part_municipio}municipio_silver.parquet")
    print("✓ Dados carregados do Silver")

    print("\n[3/3] Gerando e salvando datasets Gold...")
    df_ranking = gerar_ranking_estados(df_uf)
    df_evolucao = gerar_evolucao_temporal(df_uf)
    df_municipal = gerar_analise_municipal(df_municipio)
    df_brasil = gerar_visao_brasil(df_uf)

    salvar_parquet_s3(s3_client, df_ranking, BUCKET_GOLD,
        f"gold/ranking_estados/geracao_date={data_geracao}/ranking_estados.parquet")
    salvar_parquet_s3(s3_client, df_evolucao, BUCKET_GOLD,
        f"gold/evolucao_temporal/geracao_date={data_geracao}/evolucao_temporal.parquet")
    salvar_parquet_s3(s3_client, df_municipal, BUCKET_GOLD,
        f"gold/analise_municipal/geracao_date={data_geracao}/analise_municipal.parquet")
    salvar_parquet_s3(s3_client, df_brasil, BUCKET_GOLD,
        f"gold/visao_brasil/geracao_date={data_geracao}/visao_brasil.parquet")

    print("\n📊 PREVIEW — Ranking de Estados (2024):")
    preview = df_ranking[df_ranking["ano"] == 2024][
        ["ranking_nacional", "sigla_uf", "taxa_alfabetizacao", "situacao_meta_2030"]
    ].head(5)
    print(preview.to_string(index=False))

    print("\n📊 PREVIEW — Visão Brasil:")
    print(df_brasil.to_string(index=False))

    print(f"\n{'=' * 50}")
    print("Gold Layer concluída com sucesso!")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
