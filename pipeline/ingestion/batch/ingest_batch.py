"""
Ingestão Batch - Camada Bronze
Lê os arquivos CSV da Base dos Dados e envia para o S3 (Bronze Layer)
"""

import os
import boto3
import pandas as pd
from datetime import datetime
from pathlib import Path

# Configurações
BUCKET_NAME = "pipeline-alfabetizacao-bronze"
REGION = "us-east-1"
DATA_DIR = Path("data/raw")

# Arquivos para ingerir
ARQUIVOS = {
    "uf": "br_inep_avaliacao_alfabetizacao_uf.csv.gz",
    "meta_uf": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv.gz",
    "meta_municipio": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv.gz",
    "municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv.gz",
}

def criar_bucket_se_nao_existe(s3_client, bucket_name):
    """Cria o bucket S3 se ainda não existir."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} já existe")
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} criado")

def ingerir_arquivo(s3_client, nome_tabela, nome_arquivo):
    """
    Lê um arquivo CSV, converte para Parquet e envia ao S3 Bronze.
    Parquet é usado por ser mais eficiente que CSV (menor tamanho, mais rápido).
    """
    caminho_local = DATA_DIR / nome_arquivo

    if not caminho_local.exists():
        print(f"✗ Arquivo não encontrado: {caminho_local}")
        return False

    print(f"\n→ Ingerindo tabela: {nome_tabela}")

    # Lê o CSV
    df = pd.read_csv(caminho_local, compression='gzip')
    print(f"  Registros lidos: {len(df)}")
    print(f"  Colunas: {list(df.columns)}")

    # Define o caminho no S3 com partição por data de ingestão
    data_ingestao = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"bronze/{nome_tabela}/ingestao_date={data_ingestao}/{nome_tabela}.parquet"

    # Converte para Parquet em memória e envia ao S3
    parquet_buffer = df.to_parquet(index=False)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=parquet_buffer,
        Metadata={
            "tabela": nome_tabela,
            "registros": str(len(df)),
            "colunas": str(len(df.columns)),
            "ingestao": data_ingestao,
            "fonte": "basedosdados.org"
        }
    )

    print(f"  ✓ Enviado para s3://{BUCKET_NAME}/{s3_key}")
    print(f"  Tamanho: {len(parquet_buffer) / 1024:.1f} KB")
    return True

def main():
    print("=" * 50)
    print("INGESTÃO BATCH - CAMADA BRONZE")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # Conecta ao S3 usando as credenciais do ambiente AWS
    s3_client = boto3.client("s3", region_name=REGION)

    # Garante que o bucket existe
    criar_bucket_se_nao_existe(s3_client, BUCKET_NAME)

    # Ingere cada arquivo
    sucessos = 0
    for nome_tabela, nome_arquivo in ARQUIVOS.items():
        if ingerir_arquivo(s3_client, nome_tabela, nome_arquivo):
            sucessos += 1

    print(f"\n{'=' * 50}")
    print(f"Concluído: {sucessos}/{len(ARQUIVOS)} tabelas ingeridas")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
