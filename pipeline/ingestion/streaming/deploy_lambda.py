"""
Script de deploy da Lambda na AWS.
Empacota o código e cria/atualiza a função Lambda.
"""

import boto3
import zipfile
import io
import json

REGION = "us-east-1"
FUNCTION_NAME = "pipeline-alfabetizacao-stream-processor"
ROLE_ARN = "arn:aws:iam::379055226906:role/LabRole"

def empacotar_lambda():
    """Empacota o código da Lambda em um ZIP em memória."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("pipeline/ingestion/streaming/lambda_processor.py", "lambda_function.py")
    buffer.seek(0)
    return buffer.read()

def deploy_lambda():
    lambda_client = boto3.client("lambda", region_name=REGION)
    zip_code = empacotar_lambda()

    try:
        # Tenta atualizar se já existe
        lambda_client.update_function_code(
            FunctionName=FUNCTION_NAME,
            ZipFile=zip_code
        )
        print(f"✓ Lambda {FUNCTION_NAME} atualizada")
    except lambda_client.exceptions.ResourceNotFoundException:
        # Cria se não existe
        lambda_client.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_code},
            Timeout=30,
            MemorySize=128,
            Environment={
                "Variables": {
                    "S3_BUCKET": "pipeline-alfabetizacao-bronze",
                    "REGION": REGION
                }
            }
        )
        print(f"✓ Lambda {FUNCTION_NAME} criada")

    # Conecta o SQS como trigger da Lambda
    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = sqs_client.get_queue_url(
        QueueName="pipeline-alfabetizacao-stream"
    )["QueueUrl"]

    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    try:
        lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=FUNCTION_NAME,
            BatchSize=5,
            Enabled=True
        )
        print(f"✓ SQS conectado como trigger da Lambda")
    except lambda_client.exceptions.ResourceConflictException:
        print(f"✓ Trigger SQS já configurado")

    print("\n✓ Deploy concluído!")
    print(f"  Lambda: {FUNCTION_NAME}")
    print(f"  Trigger: SQS pipeline-alfabetizacao-stream")

if __name__ == "__main__":
    deploy_lambda()
