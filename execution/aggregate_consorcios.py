"""
aggregate_consorcios.py

Le o arquivo deals_pipedrive.json gerado pelo fetch_pipedrive_deals.py,
agrupa os deals com status "won" por consorcio, soma o kWh Contratado
e calcula a porcentagem sobre a capacidade maxima de cada consorcio.

Uso:
    python execution/aggregate_consorcios.py
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP_DIR = os.path.join(PROJECT_ROOT, ".tmp")

INPUT_FILE = os.path.join(TMP_DIR, "deals_pipedrive.json")
OUTPUT_FILE = os.path.join(TMP_DIR, "consorcios_pipedrive.json")

# Capacidade maxima em kWh — igual para todos os consorcios
CAPACIDADE_MAXIMA_KWH = 220794

# Pipelines a considerar (mesmos do script principal)
PIPELINE_KEYS = ["pipeline_36", "pipeline_37", "pipeline_38"]


# ---------------------------------------------------------------------------
# Funcao principal
# ---------------------------------------------------------------------------

def main() -> list[dict]:
    """
    Le deals_pipedrive.json, filtra deals com status 'won',
    agrupa por consorcio e calcula Valor (kWh) e Porcentagem.
    """

    # 1. Carregar dados de entrada
    if not os.path.exists(INPUT_FILE):
        print(f"ERRO: Arquivo de entrada nao encontrado: {INPUT_FILE}")
        print("Execute primeiro: python execution/fetch_pipedrive_deals.py")
        sys.exit(1)

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Arquivo carregado: {INPUT_FILE}")

    # 2. Coletar todos os deals de todos os pipelines
    all_deals: list[dict] = []
    for key in PIPELINE_KEYS:
        deals = data.get(key, [])
        all_deals.extend(deals)
        print(f"  -> {key}: {len(deals)} deal(s)")

    print(f"  -> Total: {len(all_deals)} deal(s)")

    # 3. Filtrar apenas deals com status "won"
    won_deals = [d for d in all_deals if d.get("Status") == "won"]
    print(f"  -> Deals com status 'won': {len(won_deals)}")

    # 4. Agrupar por consorcio e somar kWh Contratado
    consorcios: dict[str, float] = {}
    for deal in won_deals:
        consorcio = deal.get("Consorcio")
        kwh = deal.get("kWh Contratado")

        if not consorcio:
            continue

        if kwh is not None:
            consorcios[consorcio] = consorcios.get(consorcio, 0) + float(kwh)
        else:
            # Garante que o consorcio apareca mesmo sem kWh
            consorcios.setdefault(consorcio, 0)

    # 5. Montar resultado com porcentagem
    resultado: list[dict] = []
    for consorcio_nome in sorted(consorcios.keys()):
        valor_kwh = round(consorcios[consorcio_nome], 2)
        porcentagem = round((valor_kwh / CAPACIDADE_MAXIMA_KWH) * 100, 2)

        resultado.append({
            "Consorcio": consorcio_nome,
            "Valor (kWh)": valor_kwh,
            "Porcentagem": f"{porcentagem}%",
        })

    # 6. Adicionar metadata
    br_tz = timezone(timedelta(hours=-3))
    output = {
        "consorcios": resultado,
        "metadata": {
            "total_consorcios": len(resultado),
            "capacidade_maxima_kwh": CAPACIDADE_MAXIMA_KWH,
            "status_filtrado": "won",
            "generated_at": datetime.now(br_tz).isoformat(),
        },
    }

    # 7. Salvar
    os.makedirs(TMP_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nResultado:")
    for item in resultado:
        print(f"  {item['Consorcio']}: {item['Valor (kWh)']} kWh ({item['Porcentagem']})")

    print(f"\nSalvo em: {OUTPUT_FILE}")
    return output


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    output = main()
    print("\n" + json.dumps(output, ensure_ascii=False, indent=2))
