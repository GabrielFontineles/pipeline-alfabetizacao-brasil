"""
Silver Streaming - Consolidação de Eventos
Lê os eventos JSON gravados pela Lambda no Bronze/streaming,
consolida em tabela Silver separada dos dados oficiais.
Dados simulados nunca contaminam os indicadores reais.
"""

import boto3
import pandas as pd
import json
import io
from datetime import datetime

BUCKET_BRONZE = "pipeline-alfabetizacao-bronze"
BUCKET_SILVER = "pipeline-alfabetizacao-silver"
REGION = "us-east-1"

def listar_eventos_bronze(s3_client):
    """Lista todos os eventos JSON gravados pela Lambda."""
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_BRONZE,
        Prefix="bronze/streaming/"
    )
    if 'Contents' not in response:
        print("  Nenhum evento encontrado no Bronze/streaming")
        return []
    keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
    print(f"  Eventos encontrados: {len(keys)}")
    return keys

def ler_evento(s3_client, key):
    """Lê um evento JSON do S3."""
    response = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=key)
    return json.loads(response['Body'].read())

def consolidar_eventos(s3_client, keys):
    """Consolida todos os eventos em um DataFrame."""
    registros = []
    for key in keys:
        evento = ler_evento(s3_client, key)
        dados = evento.get("dados", {})
        registros.append({
            "evento_id": evento.get("evento_id"),
            "timestamp": evento.get("timestamp"),
            "ano": dados.get("ano"),
            "sigla_uf": dados.get("sigla_uf"),
            "rede": dados.get("rede"),
            "taxa_alfabetizacao": dados.get("taxa_alfabetizacao"),
            "media_portugues": dados.get("media_portugues"),
            "total_alunos_avaliados": dados.get("total_alunos_avaliados"),
            "fonte": dados.get("fonte")
        })
    return pd.DataFrame(registros)

def validar_e_limpar(df):
    """Valida e limpa os eventos consolidados."""
    print(f"\n  Validando {len(df)} eventos...")

    # Remove duplicatas por evento_id
    antes = len(df)
    df = df.drop_duplicates(subset=["evento_id"])
    print(f"  Duplicatas removidas: {antes - len(df)}")

    # Valida taxa entre 0 e 100
    violacoes = df[
        (df["taxa_alfabetizacao"] < 0) |
        (df["taxa_alfabetizacao"] > 100)
    ]
    if len(violacoes) > 0:
        print(f"  ⚠️ {len(violacoes)} eventos com taxa inválida — removidos")
        df = df[
            (df["taxa_alfabetizacao"] >= 0) &
            (df["taxa_alfabetizacao"] <= 100)
        ]

    # Padroniza tipos
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df["taxa_alfabetizacao"] = pd.to_numeric(df["taxa_alfabetizacao"], errors="coerce")
    df["media_portugues"] = pd.to_numeric(df["media_portugues"], errors="coerce")
    df["total_alunos_avaliados"] = pd.to_numeric(df["total_alunos_avaliados"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    print(f"  ✓ {len(df)} eventos válidos após limpeza")
    return df

def main():
    print("=" * 50)
    print("SILVER STREAMING - CONSOLIDAÇÃO")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    s3_client = boto3.client("s3", region_name=REGION)
    data_processamento = datetime.now().strftime("%Y-%m-%d")

    print("\n[1/3] Listando eventos no Bronze/streaming...")
    keys = listar_eventos_bronze(s3_client)

    if not keys:
        print("Nenhum evento para processar. Execute simulate_stream.py primeiro.")
        return

    print("\n[2/3] Consolidando e validando eventos...")
    df = consolidar_eventos(s3_client, keys)
    df = validar_e_limpar(df)

    print("\n[3/3] Salvando na Silver...")
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_key = f"silver/eventos_streaming/processamento_date={data_processamento}/eventos_streaming.parquet"
    s3_client.put_object(Bucket=BUCKET_SILVER, Key=s3_key, Body=buffer.getvalue())
    print(f"  ✓ Salvo em s3://{BUCKET_SILVER}/{s3_key}")
    print(f"  Registros: {len(df)} | Colunas: {len(df.columns)}")

    print("\n📊 PREVIEW — Eventos consolidados:")
    print(df[["evento_id", "sigla_uf", "rede", "taxa_alfabetizacao"]].head(5).to_string(index=False))

    print(f"\n{'=' * 50}")
    print("Silver Streaming concluída com sucesso!")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
