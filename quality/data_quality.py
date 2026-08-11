"""
Validação de Qualidade de Dados
Verifica completude, duplicidade, consistência e integridade
nas camadas Bronze e Silver do pipeline.
"""

import boto3
import pandas as pd
import io
import json
from datetime import datetime

REGION = "us-east-1"
BUCKET_BRONZE = "pipeline-alfabetizacao-bronze"
BUCKET_SILVER = "pipeline-alfabetizacao-silver"
DATA_INGESTAO = "2026-08-11"
DATA_PROCESSAMENTO = "2026-08-11"

def ler_parquet_s3(s3_client, bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)

def verificar_completude(df, nome_tabela, colunas_obrigatorias):
    """Verifica se colunas obrigatórias têm valores nulos."""
    print(f"\n  [Completude] {nome_tabela}")
    resultado = {"checks": [], "passou": True}

    for coluna in colunas_obrigatorias:
        nulos = df[coluna].isnull().sum()
        percentual = (nulos / len(df) * 100).round(2)
        passou = nulos == 0
        status = "✓" if passou else "✗"

        print(f"    {status} {coluna}: {nulos} nulos ({percentual}%)")
        resultado["checks"].append({
            "coluna": coluna,
            "nulos": int(nulos),
            "percentual": float(percentual),
            "passou": passou
        })
        if not passou:
            resultado["passou"] = False

    return resultado

def verificar_duplicidade(df, nome_tabela, colunas_chave):
    """Verifica registros duplicados com base nas colunas chave."""
    print(f"\n  [Duplicidade] {nome_tabela}")
    duplicatas = df.duplicated(subset=colunas_chave).sum()
    passou = duplicatas == 0
    status = "✓" if passou else "✗"
    print(f"    {status} Duplicatas encontradas: {duplicatas}")

    return {
        "duplicatas": int(duplicatas),
        "passou": passou
    }

def verificar_consistencia(df, nome_tabela, regras):
    """
    Verifica se os valores estão dentro dos limites esperados.
    regras: lista de dicts com {coluna, min, max}
    """
    print(f"\n  [Consistência] {nome_tabela}")
    resultado = {"checks": [], "passou": True}

    for regra in regras:
        coluna = regra["coluna"]
        valor_min = regra.get("min")
        valor_max = regra.get("max")

        serie = pd.to_numeric(df[coluna], errors="coerce").dropna()

        violacoes = 0
        if valor_min is not None:
            violacoes += (serie < valor_min).sum()
        if valor_max is not None:
            violacoes += (serie > valor_max).sum()

        passou = violacoes == 0
        status = "✓" if passou else "✗"
        print(f"    {status} {coluna} [{valor_min}-{valor_max}]: {violacoes} violações")

        resultado["checks"].append({
            "coluna": coluna,
            "min": valor_min,
            "max": valor_max,
            "violacoes": int(violacoes),
            "passou": passou
        })
        if not passou:
            resultado["passou"] = False

    return resultado

def verificar_integridade(df_principal, df_referencia, nome, col_principal, col_referencia):
    """Verifica se chaves de relacionamento existem na tabela de referência."""
    print(f"\n  [Integridade] {nome}")
    chaves_principal = set(df_principal[col_principal].astype(str).unique())
    chaves_referencia = set(df_referencia[col_referencia].astype(str).unique())

    orfaos = chaves_principal - chaves_referencia
    passou = len(orfaos) == 0
    status = "✓" if passou else "✗"
    print(f"    {status} Registros sem correspondência: {len(orfaos)}")
    if orfaos and len(orfaos) <= 5:
        print(f"    Exemplos: {list(orfaos)[:5]}")

    return {
        "registros_orfaos": len(orfaos),
        "passou": passou
    }

def main():
    print("=" * 50)
    print("VALIDAÇÃO DE QUALIDADE DE DADOS")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    s3_client = boto3.client("s3", region_name=REGION)

    # --- Leitura das tabelas ---
    print("\n[1/3] Carregando dados...")
    df_uf = ler_parquet_s3(s3_client, BUCKET_BRONZE,
        f"bronze/uf/ingestao_date={DATA_INGESTAO}/uf.parquet")
    df_meta_uf = ler_parquet_s3(s3_client, BUCKET_BRONZE,
        f"bronze/meta_uf/ingestao_date={DATA_INGESTAO}/meta_uf.parquet")
    df_municipio = ler_parquet_s3(s3_client, BUCKET_BRONZE,
        f"bronze/municipio/ingestao_date={DATA_INGESTAO}/municipio.parquet")
    df_meta_municipio = ler_parquet_s3(s3_client, BUCKET_BRONZE,
        f"bronze/meta_municipio/ingestao_date={DATA_INGESTAO}/meta_municipio.parquet")
    df_silver_uf = ler_parquet_s3(s3_client, BUCKET_SILVER,
        f"silver/uf/processamento_date={DATA_PROCESSAMENTO}/uf_silver.parquet")
    df_silver_municipio = ler_parquet_s3(s3_client, BUCKET_SILVER,
        f"silver/municipio/processamento_date={DATA_PROCESSAMENTO}/municipio_silver.parquet")
    print("✓ Dados carregados")

    relatorio = {
        "timestamp": datetime.now().isoformat(),
        "resultados": {}
    }

    # --- Checks Bronze ---
    print("\n[2/3] Validando camada Bronze...")

    relatorio["resultados"]["bronze_uf_completude"] = verificar_completude(
        df_uf, "bronze/uf",
        ["ano", "sigla_uf", "rede", "taxa_alfabetizacao"]
    )
    relatorio["resultados"]["bronze_uf_duplicidade"] = verificar_duplicidade(
        df_uf, "bronze/uf", ["ano", "sigla_uf", "rede"]
    )
    relatorio["resultados"]["bronze_uf_consistencia"] = verificar_consistencia(
        df_uf, "bronze/uf", [
            {"coluna": "taxa_alfabetizacao", "min": 0, "max": 100},
            {"coluna": "media_portugues", "min": 500, "max": 1000}
        ]
    )
    relatorio["resultados"]["bronze_municipio_completude"] = verificar_completude(
        df_municipio, "bronze/municipio",
        ["ano", "id_municipio", "rede", "taxa_alfabetizacao"]
    )
    relatorio["resultados"]["bronze_municipio_duplicidade"] = verificar_duplicidade(
        df_municipio, "bronze/municipio", ["ano", "id_municipio", "rede"]
    )

    # --- Checks Silver ---
    print("\n[3/3] Validando camada Silver...")

    relatorio["resultados"]["silver_uf_completude"] = verificar_completude(
        df_silver_uf, "silver/uf",
        ["ano", "sigla_uf", "rede", "taxa_alfabetizacao"]
    )
    relatorio["resultados"]["silver_municipio_completude"] = verificar_completude(
        df_silver_municipio, "silver/municipio",
        ["ano", "id_municipio", "rede", "taxa_alfabetizacao"]
    )
    relatorio["resultados"]["integridade_municipio_meta"] = verificar_integridade(
        df_municipio, df_meta_municipio,
        "municipio → meta_municipio",
        "id_municipio", "id_municipio"
    )

    # --- Resumo Final ---
    total_checks = len(relatorio["resultados"])
    checks_ok = sum(1 for r in relatorio["resultados"].values() if r.get("passou", False))

    print(f"\n{'=' * 50}")
    print(f"RESUMO: {checks_ok}/{total_checks} checks passaram")

    if checks_ok == total_checks:
        print("✓ Todos os checks de qualidade passaram!")
    else:
        print(f"✗ {total_checks - checks_ok} checks falharam — revisar dados")

    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
