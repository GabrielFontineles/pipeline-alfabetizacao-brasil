"""
Pipeline Runner — Execução Completa
Orquestra todas as etapas da pipeline na ordem correta,
com logs de tempo e status de cada etapa.
"""

import subprocess
import sys
from datetime import datetime

ETAPAS = [
    {
        "nome": "Ingestão Batch → Bronze",
        "script": "pipeline/ingestion/batch/ingest_batch.py"
    },
    {
        "nome": "Transformação → Silver",
        "script": "pipeline/silver/silver_layer.py"
    },
    {
        "nome": "Consolidação Streaming → Silver",
        "script": "pipeline/silver/silver_streaming.py"
    },
    {
        "nome": "Agregação → Gold",
        "script": "pipeline/gold/gold_layer.py"
    },
    {
        "nome": "Validação de Qualidade",
        "script": "quality/data_quality.py"
    },
    {
        "nome": "Monitoramento CloudWatch",
        "script": "monitoring/cloudwatch_alerts.py"
    },
]

def rodar_etapa(etapa):
    """Executa uma etapa da pipeline e retorna sucesso/falha."""
    inicio = datetime.now()
    print(f"\n{'='*50}")
    print(f"▶ {etapa['nome']}")
    print(f"  Script: {etapa['script']}")
    print(f"  Início: {inicio.strftime('%H:%M:%S')}")
    print(f"{'='*50}")

    resultado = subprocess.run(
        [sys.executable, etapa["script"]],
        capture_output=False
    )

    fim = datetime.now()
    duracao = (fim - inicio).seconds
    sucesso = resultado.returncode == 0
    status = "✅ SUCESSO" if sucesso else "❌ FALHA"

    print(f"\n{status} — {etapa['nome']} ({duracao}s)")
    return sucesso, duracao

def main():
    print("=" * 50)
    print("PIPELINE ALFABETIZAÇÃO BRASIL — EXECUÇÃO COMPLETA")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    resultados = []
    inicio_total = datetime.now()

    for etapa in ETAPAS:
        sucesso, duracao = rodar_etapa(etapa)
        resultados.append({
            "etapa": etapa["nome"],
            "sucesso": sucesso,
            "duracao_segundos": duracao
        })
        if not sucesso:
            print(f"\n⚠️  Pipeline interrompida na etapa: {etapa['nome']}")
            break

    fim_total = datetime.now()
    duracao_total = (fim_total - inicio_total).seconds

    print(f"\n{'='*50}")
    print("RESUMO DA EXECUÇÃO")
    print(f"{'='*50}")
    for r in resultados:
        status = "✅" if r["sucesso"] else "❌"
        print(f"  {status} {r['etapa']} ({r['duracao_segundos']}s)")

    total_ok = sum(1 for r in resultados if r["sucesso"])
    print(f"\nEtapas concluídas: {total_ok}/{len(ETAPAS)}")
    print(f"Tempo total: {duracao_total}s")
    print(f"Fim: {fim_total.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

if __name__ == "__main__":
    main()
