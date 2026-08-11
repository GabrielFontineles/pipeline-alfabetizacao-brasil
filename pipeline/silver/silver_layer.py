"""
Camada Silver - Transformação e Integração
Lê dados brutos do S3 Bronze, aplica limpeza e integração,
e salva dados tratados no S3 Silver
"""

import boto3
import pandas as pd
import io
from datetime import datetime

# Configurações
BUCKET_BRONZE = "pipeline-alfabetizacao-bronze"
BUCKET_SILVER = "pipeline-alfabetizacao-silver"
REGION = "us-east-1"
DATA_INGESTAO = "2026-08-11"

# Mapeamento de redes de ensino
MAPA_REDE = {
    "0": "Nao Identificada",
    "2": "Municipal",
    "3": "Estadual",
    "5": "Total",
    "Pública": "Publica"
}

def criar_bucket_se_nao_existe(s3_client, bucket_name):
    """Cria o bucket S3 Silver se não existir."""
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

def transformar_uf(df):
    """Limpa e padroniza a tabela UF."""
    print("\n  Aplicando transformações em UF...")

    # Padroniza coluna rede
    df["rede"] = df["rede"].astype(str).map(MAPA_REDE).fillna(df["rede"].astype(str))

    # Preenche nulos nos níveis com 0
    colunas_nivel = [c for c in df.columns if "proporcao_aluno_nivel" in c]
    df[colunas_nivel] = df[colunas_nivel].fillna(0)

    # Garante tipos corretos
    df["ano"] = df["ano"].astype(int)
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")
    df["media_portugues"] = pd.to_numeric(df["media_portugues"], errors="coerce")

    # Remove duplicatas
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  Duplicatas removidas: {antes - len(df)}")

    return df

def transformar_meta_uf(df):
    """Limpa e padroniza a tabela Meta UF."""
    print("\n  Aplicando transformações em Meta UF...")

    # Padroniza coluna rede
    df["rede"] = df["rede"].astype(str).map(MAPA_REDE).fillna(df["rede"].astype(str))

    # Garante tipos corretos
    df["ano"] = df["ano"].astype(int)
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")

    colunas_meta = [c for c in df.columns if "meta_alfabetizacao" in c]
    for col in colunas_meta:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["percentual_participacao"] = pd.to_numeric(df["percentual_participacao"], errors="coerce")

    # Remove duplicatas
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  Duplicatas removidas: {antes - len(df)}")

    return df

def transformar_municipio(df):
    """Limpa e padroniza a tabela Município."""
    print("\n  Aplicando transformações em Município...")

    # Padroniza coluna rede
    df["rede"] = df["rede"].astype(str).map(MAPA_REDE).fillna(df["rede"].astype(str))

    # Preenche nulos nos níveis com 0
    colunas_nivel = [c for c in df.columns if "proporcao_aluno_nivel" in c]
    df[colunas_nivel] = df[colunas_nivel].fillna(0)

    # Garante tipos corretos
    df["ano"] = df["ano"].astype(int)
    df["id_municipio"] = df["id_municipio"].astype(str)
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")
    df["media_portugues"] = pd.to_numeric(df["media_portugues"], errors="coerce")

    # Remove duplicatas
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  Duplicatas removidas: {antes - len(df)}")

    return df

def transformar_meta_municipio(df):
    """Limpa e padroniza a tabela Meta Município."""
    print("\n  Aplicando transformações em Meta Município...")

    # Padroniza coluna rede
    df["rede"] = df["rede"].astype(str).map(MAPA_REDE).fillna(df["rede"].astype(str))

    # Garante tipos corretos
    df["ano"] = df["ano"].astype(int)
    df["id_municipio"] = df["id_municipio"].astype(str)
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")

    colunas_meta = [c for c in df.columns if "meta_alfabetizacao" in c]
    for col in colunas_meta:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["nivel_alfabetizacao"] = pd.to_numeric(df["nivel_alfabetizacao"], errors="coerce")
    df["percentual_participacao"] = pd.to_numeric(df["percentual_participacao"], errors="coerce")

    # Remove duplicatas
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  Duplicatas removidas: {antes - len(df)}")

    return df

def integrar_uf(df_uf, df_meta_uf):
    """
    Integra UF com Meta UF.
    Resultado: uma tabela com resultado real E metas lado a lado.
    """
    print("\n  Integrando UF + Meta UF...")

    df = pd.merge(
        df_uf,
        df_meta_uf[["ano", "sigla_uf", "rede",
                     "meta_alfabetizacao_2024", "meta_alfabetizacao_2025",
                     "meta_alfabetizacao_2026", "meta_alfabetizacao_2027",
                     "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
                     "meta_alfabetizacao_2030", "percentual_participacao"]],
        on=["ano", "sigla_uf", "rede"],
        how="left"
    )

    # Calcula distância da meta atual
    df["distancia_meta_2030"] = df["meta_alfabetizacao_2030"] - df["taxa_alfabetizacao"]

    print(f"  Registros integrados: {len(df)}")
    return df

def integrar_municipio(df_municipio, df_meta_municipio):
    """
    Integra Município com Meta Município.
    Resultado: uma tabela com resultado real E metas por cidade.
    """
    print("\n  Integrando Município + Meta Município...")

    df = pd.merge(
        df_municipio,
        df_meta_municipio[["ano", "id_municipio", "rede",
                            "meta_alfabetizacao_2024", "meta_alfabetizacao_2025",
                            "meta_alfabetizacao_2026", "meta_alfabetizacao_2027",
                            "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
                            "meta_alfabetizacao_2030", "nivel_alfabetizacao",
                            "percentual_participacao"]],
        on=["ano", "id_municipio", "rede"],
        how="left"
    )

    # Calcula distância da meta atual
    df["distancia_meta_2030"] = df["meta_alfabetizacao_2030"] - df["taxa_alfabetizacao"]

    print(f"  Registros integrados: {len(df)}")
    return df

def main():
    print("=" * 50)
    print("TRANSFORMAÇÃO SILVER LAYER")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    s3_client = boto3.client("s3", region_name=REGION)
    criar_bucket_se_nao_existe(s3_client, BUCKET_SILVER)

    data_processamento = datetime.now().strftime("%Y-%m-%d")

    # --- Leitura do Bronze ---
    print("\n[1/4] Lendo dados do Bronze...")
    df_uf = ler_parquet_s3(s3_client, BUCKET_BRONZE,
        f"bronze/uf/ingestao_date={DATA_INGESTAO}/uf.parquet")
    df_meta_uf = ler_parquet_s3(s3_client, BUCKET_BRONZE,
        f"bronze/meta_uf/ingestao_date={DATA_INGESTAO}/meta_uf.parquet")
    df_municipio = ler_parquet_s3(s3_client, BUCKET_BRONZE,
        f"bronze/municipio/ingestao_date={DATA_INGESTAO}/municipio.parquet")
    df_meta_municipio = ler_parquet_s3(s3_client, BUCKET_BRONZE,
        f"bronze/meta_municipio/ingestao_date={DATA_INGESTAO}/meta_municipio.parquet")
    print("✓ Dados carregados do Bronze")

    # --- Transformações ---
    print("\n[2/4] Aplicando transformações...")
    df_uf = transformar_uf(df_uf)
    df_meta_uf = transformar_meta_uf(df_meta_uf)
    df_municipio = transformar_municipio(df_municipio)
    df_meta_municipio = transformar_meta_municipio(df_meta_municipio)

    # --- Integração ---
    print("\n[3/4] Integrando tabelas...")
    df_silver_uf = integrar_uf(df_uf, df_meta_uf)
    df_silver_municipio = integrar_municipio(df_municipio, df_meta_municipio)

    # --- Salvar no Silver ---
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
