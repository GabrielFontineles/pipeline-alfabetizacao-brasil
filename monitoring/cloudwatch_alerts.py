"""
Monitoramento com CloudWatch
Publica métricas customizadas e cria alarmes para o pipeline.
"""

import boto3
from datetime import datetime

REGION = "us-east-1"
NAMESPACE = "PipelineAlfabetizacao"

cloudwatch = boto3.client("cloudwatch", region_name=REGION)

def publicar_metrica(nome, valor, unidade, dimensoes=None):
    """Publica uma métrica customizada no CloudWatch."""
    metric = {
        "MetricName": nome,
        "Value": valor,
        "Unit": unidade,
        "Timestamp": datetime.now()
    }
    if dimensoes:
        metric["Dimensions"] = dimensoes

    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[metric]
    )
    print(f"  ✓ Métrica publicada: {nome} = {valor} {unidade}")

def publicar_metricas_ingestao(tabela, registros, tempo_segundos, sucesso=True):
    """Publica métricas de uma execução de ingestão batch."""
    dimensoes = [{"Name": "Tabela", "Value": tabela}]

    publicar_metrica("RegistrosIngeridos", registros, "Count", dimensoes)
    publicar_metrica("TempoIngestao", tempo_segundos, "Seconds", dimensoes)
    publicar_metrica("IngestaoSucesso", 1 if sucesso else 0, "Count", dimensoes)

def publicar_metricas_qualidade(checks_total, checks_ok):
    """Publica métricas dos checks de qualidade."""
    checks_falhos = checks_total - checks_ok
    taxa_sucesso = (checks_ok / checks_total * 100) if checks_total > 0 else 0

    publicar_metrica("QualidadeChecksTotal", checks_total, "Count")
    publicar_metrica("QualidadeChecksFalhos", checks_falhos, "Count")
    publicar_metrica("QualidadeTaxaSucesso", taxa_sucesso, "Percent")

def publicar_metricas_streaming(eventos_enviados, eventos_processados):
    """Publica métricas do pipeline de streaming."""
    publicar_metrica("StreamingEventosEnviados", eventos_enviados, "Count")
    publicar_metrica("StreamingEventosProcessados", eventos_processados, "Count")
    taxa = (eventos_processados / eventos_enviados * 100) if eventos_enviados > 0 else 0
    publicar_metrica("StreamingTaxaProcessamento", taxa, "Percent")

def criar_alarme_qualidade():
    """
    Cria alarme que dispara quando checks de qualidade falham.
    Em produção, enviaria email ou SMS via SNS.
    """
    try:
        cloudwatch.put_metric_alarm(
            AlarmName="Pipeline-QualidadeDados-Falha",
            AlarmDescription="Alarme disparado quando checks de qualidade falham",
            MetricName="QualidadeChecksFalhos",
            Namespace=NAMESPACE,
            Statistic="Sum",
            Period=3600,
            EvaluationPeriods=1,
            Threshold=1,
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            TreatMissingData="notBreaching"
        )
        print("  ✓ Alarme de qualidade criado")
    except Exception as e:
        print(f"  ⚠ Alarme já existe ou erro: {str(e)[:50]}")

def criar_alarme_ingestao():
    """
    Cria alarme que dispara quando ingestão falha.
    """
    try:
        cloudwatch.put_metric_alarm(
            AlarmName="Pipeline-Ingestao-Falha",
            AlarmDescription="Alarme disparado quando ingestão batch falha",
            MetricName="IngestaoSucesso",
            Namespace=NAMESPACE,
            Statistic="Minimum",
            Period=3600,
            EvaluationPeriods=1,
            Threshold=1,
            ComparisonOperator="LessThanThreshold",
            TreatMissingData="notBreaching"
        )
        print("  ✓ Alarme de ingestão criado")
    except Exception as e:
        print(f"  ⚠ Alarme já existe ou erro: {str(e)[:50]}")

def main():
    print("=" * 50)
    print("CONFIGURAÇÃO DE MONITORAMENTO")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # Publica métricas da última execução do pipeline
    print("\n[1/4] Publicando métricas de ingestão batch...")
    publicar_metricas_ingestao("uf", 145, 1.2)
    publicar_metricas_ingestao("meta_uf", 54, 0.8)
    publicar_metricas_ingestao("meta_municipio", 10704, 2.1)
    publicar_metricas_ingestao("municipio", 23995, 2.8)

    print("\n[2/4] Publicando métricas de qualidade...")
    publicar_metricas_qualidade(checks_total=8, checks_ok=7)

    print("\n[3/4] Publicando métricas de streaming...")
    publicar_metricas_streaming(eventos_enviados=20, eventos_processados=20)

    print("\n[4/4] Criando alarmes CloudWatch...")
    criar_alarme_qualidade()
    criar_alarme_ingestao()

    print(f"\n{'=' * 50}")
    print("✓ Monitoramento configurado com sucesso!")
    print(f"  Namespace: {NAMESPACE}")
    print(f"  Acesse: CloudWatch > Metrics > {NAMESPACE}")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
