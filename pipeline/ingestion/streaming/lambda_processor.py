"""
Lambda - Processador de Eventos SQS
Triggered automaticamente pelo SQS.
Lê cada evento de resultado de alfabetização e salva no S3 Bronze.
"""

import json
import boto3
import os
from datetime import datetime

S3_BUCKET = "pipeline-alfabetizacao-bronze"
REGION = "us-east-1"

s3_client = boto3.client("s3", region_name=REGION)

def lambda_handler(event, context):
    """
    Função principal da Lambda.
    Recebe eventos do SQS e persiste no S3 Bronze.
    """
    print(f"Recebidos {len(event['Records'])} registros do SQS")

    processados = 0
    erros = 0

    for record in event["Records"]:
        try:
            # Lê o corpo da mensagem SQS
            body = json.loads(record["body"])

            evento_id = body["evento_id"]
            timestamp = body["timestamp"]
            dados = body["dados"]

            # Define o caminho no S3 particionado por data e UF
            data_evento = timestamp[:10]
            sigla_uf = dados["sigla_uf"]
            s3_key = (
                f"bronze/streaming/"
                f"ano={dados['ano']}/"
                f"uf={sigla_uf}/"
                f"data={data_evento}/"
                f"{evento_id}.json"
            )

            # Salva o evento no S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(body, ensure_ascii=False),
                ContentType="application/json",
                Metadata={
                    "evento_id": evento_id,
                    "sigla_uf": sigla_uf,
                    "data_evento": data_evento
                }
            )

            print(f"✓ Evento {evento_id} salvo em s3://{S3_BUCKET}/{s3_key}")
            processados += 1

        except Exception as e:
            print(f"✗ Erro ao processar evento: {str(e)}")
            erros += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processados": processados,
            "erros": erros,
            "timestamp": datetime.now().isoformat()
        })
    }
