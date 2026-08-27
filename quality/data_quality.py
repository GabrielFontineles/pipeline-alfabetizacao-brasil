"""
Validação de Qualidade de Dados
Verifica completude, duplicidade, consistência e integridade
nas camadas Bronze e Silver do pipeline.
Salva relatório JSON no S3 para auditoria.
"""

import boto3
import pandas as pd
import io
import json
from datetime import datetime

REGION = "us-east-1"
BUCKET_BRONZE = "pipeline-alfabetizacao-bronze"
BUCKET_SILVER = "pipeline-alfabetizacao-silver"

def ler_parquet_s3(s3_client, bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)

def descobrir_particao_recente(s3_client, bucket, prefixo):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo, Delimiter='/')
    if 'CommonPrefixes' not in response:
        raise ValueError(f"Nenhuma partição encontrada em s3://{bucket}/{prefixo}")
    particoes = [p['Prefix'] for p in response['CommonPrefixes']]
    return sorted(particoes)[-1]

def verificar_completude(df, nome_tabela, colunas_obrigatorias):
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
    print(f"\n  [Duplicidade] {nome_tabela}")
    duplicatas = df.duplicated(subset=colunas_chave).sum()
    passou = duplicatas == 0
    status = "✓" if passou else "✗"
    print(f"    {status} Duplicatas encontradas: {duplicatas}")
    return {"duplicatas": int(duplicatas), "passou": passou}

def verificar_consistencia(df, nome_tabela, regras):
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
    print(f"\n  [Integridade] {nome}")
    chaves_principal = set(df_principal[col_principal].astype(str).unique())
    chaves_referencia = set(df_referencia[col_referencia].astype(str).unique())
    orfaos = chaves_principal - chaves_referencia
    passou = len(orfaos) == 0
    status = "✓" if passou else "✗"
    print(f"    {status} Registros sem correspondência: {len(orfaos)}")
    return {"registros_orfaos": len(orfaos), "passou": passou}

def salvar_relatorio(s3_client, relatorio):
    """Salva o relatório de qualidade como JSON no S3 para auditoria."""
    data = datetime.now().strftime("%Y-%m-%d")
    key = f"quality-reports/data_quality_report_{data}.json"
    s3_client.put_object(
        Bucket=BUCKET_SILVER,
        Key=key,
        Body=json.dumps(relatorio, indent=2, ensure_ascii=False),
        ContentType="application/json"
    )
    print(f"\n  ✓ Relatório salvo em s3://{BUCKET_SILVER}/{key}")

def main():
    print("=" * 50)
    print("VALIDAÇÃO DE QUALIDADE DE DADOS")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    s3_client = boto3.client("s3", region_name=REGION)

    print("\n[1/3] Descobrindo partições e carregando dados...")
    part_uf = descobrir_particao_recente(s3_client, BUCKET_BRONZE, "bronze/uf/")
    part_municipio = descobrir_particao_recente(s3_client, BUCKET_BRONZE, "bronze/municipio/")
    part_meta_municipio = descobrir_particao_recente(s3_client, BUCKET_BRONZE, "bronze/meta_municipio/")
    part_silver_uf = descobrir_particao_recente(s3_client, BUCKET_SILVER, "silver/uf/")
    part_silver_municipio = descobrir_particao_recente(s3_client, BUCKET_SILVER, "silver/municipio/")

    df_uf = ler_parquet_s3(s3_client, BUCKET_BRONZE, f"{part_uf}uf.parquet")
    df_municipio = ler_parquet_s3(s3_client, BUCKET_BRONZE, f"{part_municipio}municipio.parquet")
    df_meta_municipio = ler_parquet_s3(s3_client, BUCKET_BRONZE, f"{part_meta_municipio}meta_municipio.parquet")
    df_silver_uf = ler_parquet_s3(s3_client, BUCKET_SILVER, f"{part_silver_uf}uf_silver.parquet")
    df_silver_municipio = ler_parquet_s3(s3_client, BUCKET_SILVER, f"{part_silver_municipio}municipio_silver.parquet")
    print("✓ Dados carregados")

    relatorio = {
        "timestamp": datetime.now().isoformat(),
        "resultados": {}
    }

    print("\n[2/3] Executando checks...")
    relatorio["resultados"]["bronze_uf_completude"] = verificar_completude(
        df_uf, "bronze/uf", ["ano", "sigla_uf", "rede", "taxa_alfabetizacao"])
    relatorio["resultados"]["bronze_uf_duplicidade"] = verificar_duplicidade(
        df_uf, "bronze/uf", ["ano", "sigla_uf", "rede"])
    relatorio["resultados"]["bronze_uf_consistencia"] = verificar_consistencia(
        df_uf, "bronze/uf", [
            {"coluna": "taxa_alfabetizacao", "min": 0, "max": 100},
            {"coluna": "media_portugues", "min": 500, "max": 1000}
        ])
    relatorio["resultados"]["bronze_municipio_completude"] = verificar_completude(
        df_municipio, "bronze/municipio", ["ano", "id_municipio", "rede", "taxa_alfabetizacao"])
    relatorio["resultados"]["bronze_municipio_duplicidade"] = verificar_duplicidade(
        df_municipio, "bronze/municipio", ["ano", "id_municipio", "rede"])
    relatorio["resultados"]["silver_uf_completude"] = verificar_completude(
        df_silver_uf, "silver/uf", ["ano", "sigla_uf", "rede", "taxa_alfabetizacao"])
    relatorio["resultados"]["silver_municipio_completude"] = verificar_completude(
        df_silver_municipio, "silver/municipio", ["ano", "id_municipio", "rede", "taxa_alfabetizacao"])
    relatorio["resultados"]["integridade_municipio_meta"] = verificar_integridade(
        df_municipio, df_meta_municipio, "municipio → meta_municipio",
        "id_municipio", "id_municipio")

    total_checks = len(relatorio["resultados"])
    checks_ok = sum(1 for r in relatorio["resultados"].values() if r.get("passou", False))
    relatorio["resumo"] = {
        "total_checks": total_checks,
        "checks_aprovados": checks_ok,
        "checks_reprovados": total_checks - checks_ok,
        "taxa_aprovacao": round(checks_ok / total_checks * 100, 1)
    }

    print(f"\n[3/3] Salvando relatório no S3...")
    salvar_relatorio(s3_client, relatorio)

    print(f"\n{'='*50}")
    print(f"RESUMO: {checks_ok}/{total_checks} checks passaram ({relatorio['resumo']['taxa_aprovacao']}%)")
    if checks_ok == total_checks:
        print("✓ Todos os checks de qualidade passaram!")
    else:
        print(f"✗ {total_checks - checks_ok} checks falharam — revisar dados")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
