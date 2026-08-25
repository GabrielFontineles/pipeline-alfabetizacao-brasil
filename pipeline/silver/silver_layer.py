"""
Camada Silver - Transformação e Integração
Lê dados brutos do S3 Bronze, aplica limpeza e integração,
e salva dados tratados no S3 Silver.
Partição descoberta dinamicamente — não depende de data fixa.
"""

import boto3
import pandas as pd
import io
from datetime import datetime

BUCKET_BRONZE = "pipeline-alfabetizacao-bronze"
BUCKET_SILVER = "pipeline-alfabetizacao-silver"
REGION = "us-east-1"

MAPA_REDE = {
    "0": "Nao Identificada",
    "2": "Municipal",
    "3": "Estadual",
    "5": "Total",
    "Pública": "Publica",
    "Municipal": "Municipal",
    "Estadual": "Estadual",
    "Total": "Total"
}

def criar_bucket_se_nao_existe(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} já existe")
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} criado")

def descobrir_particao_recente(s3_client, bucket, prefixo):
    """
    Descobre dinamicamente a partição mais recente no S3.
    Evita datas fixas no código que quebram em execuções futuras.
    """
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

def transformar_uf(df):
    print("\n  Aplicando transformações em UF...")
    df["rede"] = df["rede"].astype(str).map(MAPA_REDE).fillna(df["rede"].astype(str))
    colunas_nivel = [c for c in df.columns if "proporcao_aluno_nivel" in c]
    df[colunas_nivel] = df[colunas_nivel].fillna(0)
    df["ano"] = df["ano"].astype(int)
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")
    df["media_portugues"] = pd.to_numeric(df["media_portugues"], errors="coerce")
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  Duplicatas removidas: {antes - len(df)}")
    return df

def transformar_meta_uf(df):
    print("\n  Aplicando transformações em Meta UF...")
    df["rede_meta"] = df["rede"].astype(str)
    df["ano"] = df["ano"].astype(int)
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")
    colunas_meta = [c for c in df.columns if "meta_alfabetizacao" in c]
    for col in colunas_meta:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["percentual_participacao"] = pd.to_numeric(df["percentual_participacao"], errors="coerce")
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  Duplicatas removidas: {antes - len(df)}")
    return df

def transformar_municipio(df):
    print("\n  Aplicando transformações em Município...")
    df["rede"] = df["rede"].astype(str).map(MAPA_REDE).fillna(df["rede"].astype(str))
    colunas_nivel = [c for c in df.columns if "proporcao_aluno_nivel" in c]
    df[colunas_nivel] = df[colunas_nivel].fillna(0)
    df["ano"] = df["ano"].astype(int)
    df["id_municipio"] = df["id_municipio"].astype(str)
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")
    df["media_portugues"] = pd.to_numeric(df["media_portugues"], errors="coerce")
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  Duplicatas removidas: {antes - len(df)}")
    return df

def transformar_meta_municipio(df):
    print("\n  Aplicando transformações em Meta Município...")
    df["rede_meta"] = df["rede"].astype(str)
    df["ano"] = df["ano"].astype(int)
    df["id_municipio"] = df["id_municipio"].astype(str)
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")
    colunas_meta = [c for c in df.columns if "meta_alfabetizacao" in c]
    for col in colunas_meta:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["nivel_alfabetizacao"] = pd.to_numeric(df["nivel_alfabetizacao"], errors="coerce")
    df["percentual_participacao"] = pd.to_numeric(df["percentual_participacao"], errors="coerce")
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  Duplicatas removidas: {antes - len(df)}")
    return df

def integrar_uf(df_uf, df_meta_uf):
    """
    Integra UF com Meta UF por ano e sigla_uf (sem join por rede,
    pois as tabelas usam categorias incompatíveis: indicador usa
    Municipal/Estadual/Total enquanto meta usa Pública).
    """
    print("\n  Integrando UF + Meta UF...")
    df_meta_agg = df_meta_uf.groupby(["ano", "sigla_uf"]).agg(
        meta_alfabetizacao_2024=("meta_alfabetizacao_2024", "first"),
        meta_alfabetizacao_2025=("meta_alfabetizacao_2025", "first"),
        meta_alfabetizacao_2026=("meta_alfabetizacao_2026", "first"),
        meta_alfabetizacao_2027=("meta_alfabetizacao_2027", "first"),
        meta_alfabetizacao_2028=("meta_alfabetizacao_2028", "first"),
        meta_alfabetizacao_2029=("meta_alfabetizacao_2029", "first"),
        meta_alfabetizacao_2030=("meta_alfabetizacao_2030", "first"),
        percentual_participacao=("percentual_participacao", "first")
    ).reset_index()

    df = pd.merge(df_uf, df_meta_agg, on=["ano", "sigla_uf"], how="left")
    df["distancia_meta_2030"] = df["meta_alfabetizacao_2030"] - df["taxa_alfabetizacao"]
    metas_preenchidas = df["meta_alfabetizacao_2030"].notna().sum()
    print(f"  Registros integrados: {len(df)}")
    print(f"  Metas preenchidas: {metas_preenchidas}/{len(df)} ({metas_preenchidas/len(df)*100:.1f}%)")
    return df

def integrar_municipio(df_municipio, df_meta_municipio):
    """
    Integra Município com Meta por ano e id_municipio (sem join por rede).
    """
    print("\n  Integrando Município + Meta Município...")
    df_meta_agg = df_meta_municipio.groupby(["ano", "id_municipio"]).agg(
        meta_alfabetizacao_2024=("meta_alfabetizacao_2024", "first"),
        meta_alfabetizacao_2025=("meta_alfabetizacao_2025", "first"),
        meta_alfabetizacao_2026=("meta_alfabetizacao_2026", "first"),
        meta_alfabetizacao_2027=("meta_alfabetizacao_2027", "first"),
        meta_alfabetizacao_2028=("meta_alfabetizacao_2028", "first"),
        meta_alfabetizacao_2029=("meta_alfabetizacao_2029", "first"),
        meta_alfabetizacao_2030=("meta_alfabetizacao_2030", "first"),
        nivel_alfabetizacao=("nivel_alfabetizacao", "first"),
        percentual_participacao=("percentual_participacao", "first")
    ).reset_index()

    df = pd.merge(df_municipio, df_meta_agg, on=["ano", "id_municipio"], how="left")
    df["distancia_meta_2030"] = df["meta_alfabetizacao_2030"] - df["taxa_alfabetizacao"]
    metas_preenchidas = df["meta_alfabetizacao_2030"].notna().sum()
    print(f"  Registros integrados: {len(df)}")
    print(f"  Metas preenchidas: {metas_preenchidas}/{len(df)} ({metas_preenchidas/len(df)*100:.1f}%)")
    return df

def main():
    print("=" * 50)
    print("TRANSFORMAÇÃO SILVER LAYER")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    s3_client = boto3.client("s3", region_name=REGION)
    criar_bucket_se_nao_existe(s3_client, BUCKET_SILVER)
    data_processamento = datetime.now().strftime("%Y-%m-%d")

    print("\n[1/4] Descobrindo partições no Bronze...")
    part_uf = descobrir_particao_recente(s3_client, BUCKET_BRONZE, "bronze/uf/")
    part_meta_uf = descobrir_particao_recente(s3_client, BUCKET_BRONZE, "bronze/meta_uf/")
    part_municipio = descobrir_particao_recente(s3_client, BUCKET_BRONZE, "bronze/municipio/")
    part_meta_municipio = descobrir_particao_recente(s3_client, BUCKET_BRONZE, "bronze/meta_municipio/")

    print("\n[2/4] Lendo dados do Bronze...")
    df_uf = ler_parquet_s3(s3_client, BUCKET_BRONZE, f"{part_uf}uf.parquet")
    df_meta_uf = ler_parquet_s3(s3_client, BUCKET_BRONZE, f"{part_meta_uf}meta_uf.parquet")
    df_municipio = ler_parquet_s3(s3_client, BUCKET_BRONZE, f"{part_municipio}municipio.parquet")
    df_meta_municipio = ler_parquet_s3(s3_client, BUCKET_BRONZE, f"{part_meta_municipio}meta_municipio.parquet")
    print("✓ Dados carregados do Bronze")

    print("\n[3/4] Aplicando transformações e integração...")
    df_uf = transformar_uf(df_uf)
    df_meta_uf = transformar_meta_uf(df_meta_uf)
    df_municipio = transformar_municipio(df_municipio)
    df_meta_municipio = transformar_meta_municipio(df_meta_municipio)
    df_silver_uf = integrar_uf(df_uf, df_meta_uf)
    df_silver_municipio = integrar_municipio(df_municipio, df_meta_municipio)

    print("\n[4/4] Salvando no S3 Silver...")
    salvar_parquet_s3(s3_client, df_silver_uf, BUCKET_SILVER,
        f"silver/uf/processamento_date={data_processamento}/uf_silver.parquet")
    salvar_parquet_s3(s3_client, df_silver_municipio, BUCKET_SILVER,
        f"silver/municipio/processamento_date={data_processamento}/municipio_silver.parquet")

    print(f"\n{'=' * 50}")
    print("Silver Layer concluída com sucesso!")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
