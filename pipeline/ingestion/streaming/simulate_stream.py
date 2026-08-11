"""
Simulação de Streaming - Gerador de Eventos
Simula a chegada de novos resultados de alfabetização em tempo quase real,
enviando mensagens para uma fila SQS que serão processadas por uma Lambda.
"""

import boto3
import json
import random
import time
from datetime import datetime

# Configurações
REGION = "us-east-1"
QUEUE_NAME = "pipeline-alfabetizacao-stream"

# Dados de referência para simulação
ESTADOS = [
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA",
    "MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN",
    "RO","RR","RS","SC","SE","SP","TO"
]

REDES = ["Municipal", "Estadual"]

def criar_fila_se_nao_existe(sqs_client, queue_name):
    """Cria a fila SQS se não existir."""
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
        print(f"✓ Fila {queue_name} já existe")
        return response["QueueUrl"]
    except sqs_client.exceptions.QueueDoesNotExist:
        response = sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={"MessageRetentionPeriod": "86400"}  # 1 dia
        )
        print(f"✓ Fila {queue_name} criada")
        return response["QueueUrl"]

def gerar_evento():
    """
    Gera um evento simulado de novo resultado de alfabetização.
    Simula como seria receber dados de uma avaliação em tempo real.
    """
    sigla_uf = random.choice(ESTADOS)
    rede = random.choice(REDES)

    # Gera taxa realista baseada em distribuição brasileira
    taxa_base = random.gauss(55, 15)
    taxa = max(10, min(95, taxa_base))

    evento = {
        "evento_id": f"evt_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random.randint(1000, 9999)}",
        "timestamp": datetime.now().isoformat(),
        "tipo": "novo_resultado_alfabetizacao",
        "dados": {
            "ano": 2024,
            "sigla_uf": sigla_uf,
            "rede": rede,
            "taxa_alfabetizacao": round(taxa, 2),
            "media_portugues": round(random.gauss(750, 30), 2),
            "total_alunos_avaliados": random.randint(100, 5000),
            "fonte": "simulacao_streaming"
        }
    }
    return evento

def enviar_evento(sqs_client, queue_url, evento):
    """Envia um evento para a fila SQS."""
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(evento),
        MessageAttributes={
            "tipo_evento": {
                "StringValue": evento["tipo"],
                "DataType": "String"
            },
            "sigla_uf": {
                "StringValue": evento["dados"]["sigla_uf"],
                "DataType": "String"
            }
        }
    )

def main(num_eventos=20, intervalo_segundos=1):
    print("=" * 50)
    print("SIMULAÇÃO DE STREAMING")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Eventos a enviar: {num_eventos}")
    print("=" * 50)

    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = criar_fila_se_nao_existe(sqs_client, QUEUE_NAME)

    print(f"\nEnviando {num_eventos} eventos...\n")

    for i in range(1, num_eventos + 1):
        evento = gerar_evento()
        enviar_evento(sqs_client, queue_url, evento)

        print(f"[{i:02d}/{num_eventos}] Evento enviado:")
        print(f"  ID: {evento['evento_id']}")
        print(f"  UF: {evento['dados']['sigla_uf']} | "
              f"Rede: {evento['dados']['rede']} | "
              f"Taxa: {evento['dados']['taxa_alfabetizacao']}%")

        if i < num_eventos:
            time.sleep(intervalo_segundos)

    print(f"\n{'=' * 50}")
    print(f"✓ {num_eventos} eventos enviados para a fila SQS")
    print(f"Fila: {queue_url}")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main(num_eventos=20, intervalo_segundos=1)
