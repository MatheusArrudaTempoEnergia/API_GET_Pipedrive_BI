"""
fetch_pipedrive_deals.py

Busca deals dos pipelines 36, 37, 38 na API do Pipedrive (v1),
extrai campos personalizados mapeados, resolve IDs de campos
enum/set/stage para seus nomes e retorna em JSON.

Uso:
    python execution/fetch_pipedrive_deals.py
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

# Carrega .env a partir da raiz do projeto (um nivel acima de execution/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
BASE_URL = os.getenv("PIPEDRIVE_BASE_URL")

if not API_TOKEN or not BASE_URL:
    print("ERRO: PIPEDRIVE_API_TOKEN e PIPEDRIVE_BASE_URL devem estar definidos no .env")
    sys.exit(1)

# Pipelines a consultar
PIPELINE_IDS = [36, 37, 38]

# Mapeamento: chave da API -> nome amigavel
FIELD_MAP = {
    "title": "Titulo",
    "stage_id": "Etapa",
    "value": "Valor",
    "status": "Status",
    "5d6f2509ce01acf1f143dde2bd8b9bfbc22fd3c1": "Percentual Concedido",
    "bc6e85bfd61a7f514b65fbf8d1f3a7bacefc7f56": "Data de Alteracao de Funil",
    "fae8184ad9ee4befb23365ad84e47c76e03c6f71": "Tipo do Plano",
    "93df664878ce08f58067f382e1c134bed803ce53": "Finder (Origem da Fatura)",
    "b4233a37174ad172b79ec854faab5d280ec78fa3": "Lead (Origem)",
    "deb4d6ce978779304d95add8260d43d14051d6a1": "N de Unidades",
    "9fe715b9c83f91c5131aa7cf580c20f033912228": "Media de Consumo (KWh)",
    "bba2ac4fe94f03ecdd992fb776f72920365333ac": "Consorcio",
    "ca61a683d1602938a67b5431d929affc35a8c486": "kWh Contratado",
    "f6671d52cf7acaa5c7ee0370fd43e064078f913e": "kWh Nao Compensavel",
    "f29736fac633e87f54f381b99b362adb1e7bb0ee": "Cidade da Instalacao",
}

# Campos do tipo "opcao unica" (enum) — retornam um ID numerico
ENUM_FIELDS = [
    "fae8184ad9ee4befb23365ad84e47c76e03c6f71",  # Tipo do Plano
    "bba2ac4fe94f03ecdd992fb776f72920365333ac",  # Consorcio
]

# Campos do tipo "multipla escolha" (set) — retornam IDs separados por virgula
SET_FIELDS = [
    "93df664878ce08f58067f382e1c134bed803ce53",  # Finder (Origem da Fatura)
]

# Paginacao — maximo permitido pela API
PAGE_LIMIT = 1000


# ---------------------------------------------------------------------------
# Funcoes auxiliares — resolucao de IDs para nomes
# ---------------------------------------------------------------------------

def fetch_deal_field_options() -> dict[str, dict[int, str]]:
    """
    Busca os dealFields da API e monta um dicionario de opcoes
    para os campos enum e set que precisamos resolver.

    Retorna: { field_key: { option_id: option_label, ... }, ... }
    """
    fields_to_resolve = set(ENUM_FIELDS + SET_FIELDS)
    options_map: dict[str, dict[int, str]] = {}

    start = 0
    while True:
        url = f"{BASE_URL}/dealFields"
        params = {
            "api_token": API_TOKEN,
            "start": start,
            "limit": PAGE_LIMIT,
        }
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if not data.get("success"):
            print("AVISO: Nao foi possivel buscar dealFields")
            break

        fields_raw = data.get("data") or []
        if not fields_raw:
            break

        for field in fields_raw:
            field_key = field.get("key")
            if field_key in fields_to_resolve and field.get("options"):
                options_map[field_key] = {
                    opt["id"]: opt["label"]
                    for opt in field["options"]
                }

        # Paginacao
        pagination = data.get("additional_data", {}).get("pagination", {})
        if pagination.get("more_items_in_collection"):
            start = pagination.get("next_start", start + PAGE_LIMIT)
        else:
            break

    return options_map


def fetch_stages_map() -> dict[int, str]:
    """
    Busca as etapas (stages) de todos os pipelines configurados.

    Retorna: { stage_id: stage_name, ... }
    """
    stages_map: dict[int, str] = {}

    for pipeline_id in PIPELINE_IDS:
        url = f"{BASE_URL}/stages"
        params = {
            "api_token": API_TOKEN,
            "pipeline_id": pipeline_id,
        }
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if not data.get("success"):
            print(f"AVISO: Nao foi possivel buscar stages do pipeline {pipeline_id}")
            continue

        for stage in (data.get("data") or []):
            stages_map[stage["id"]] = stage["name"]

    return stages_map


def resolve_field_value(
    api_key: str,
    raw_value,
    options_map: dict[str, dict[int, str]],
    stages_map: dict[int, str],
) -> any:
    """
    Resolve o valor bruto de um campo para seu nome legivel.

    - stage_id: busca no stages_map
    - ENUM_FIELDS: converte ID numerico para label
    - SET_FIELDS: converte IDs separados por virgula para labels
    - Outros campos: retorna o valor original
    """
    if raw_value is None:
        return None

    # Etapa (stage_id)
    if api_key == "stage_id":
        return stages_map.get(int(raw_value), raw_value)

    # Opcao unica (enum)
    if api_key in ENUM_FIELDS:
        field_options = options_map.get(api_key, {})
        try:
            return field_options.get(int(raw_value), raw_value)
        except (ValueError, TypeError):
            return raw_value

    # Multipla escolha (set) — IDs separados por virgula
    if api_key in SET_FIELDS:
        field_options = options_map.get(api_key, {})
        try:
            ids = str(raw_value).split(",")
            labels = []
            for id_str in ids:
                id_int = int(id_str.strip())
                labels.append(field_options.get(id_int, id_str.strip()))
            return ", ".join(labels)
        except (ValueError, TypeError):
            return raw_value

    return raw_value


# ---------------------------------------------------------------------------
# Funcao principal — busca de deals
# ---------------------------------------------------------------------------

def fetch_deals_from_pipeline(
    pipeline_id: int,
    options_map: dict[str, dict[int, str]],
    stages_map: dict[int, str],
) -> list[dict]:
    """
    Busca TODOS os deals de um pipeline, tratando paginacao automatica.
    Resolve IDs de campos enum/set/stage para nomes legiveis.
    Retorna lista de deals ja filtrados com os campos mapeados.
    """
    all_deals: list[dict] = []
    start = 0

    while True:
        url = f"{BASE_URL}/pipelines/{pipeline_id}/deals"
        params = {
            "api_token": API_TOKEN,
            "start": start,
            "limit": PAGE_LIMIT,
            "everyone": 1,       # Buscar deals de todos os usuarios
        }

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()

        if not data.get("success"):
            print(f"AVISO: API retornou success=false para pipeline {pipeline_id} (start={start})")
            break

        deals_raw = data.get("data") or []
        if not deals_raw:
            break

        # Extrai e resolve os campos mapeados de cada deal
        for deal in deals_raw:
            mapped_deal = {"id": deal.get("id")}
            for api_key, friendly_name in FIELD_MAP.items():
                raw_value = deal.get(api_key)
                mapped_deal[friendly_name] = resolve_field_value(
                    api_key, raw_value, options_map, stages_map
                )
            all_deals.append(mapped_deal)

        # Verifica se ha mais paginas
        pagination = data.get("additional_data", {}).get("pagination", {})
        if pagination.get("more_items_in_collection"):
            start = pagination.get("next_start", start + PAGE_LIMIT)
        else:
            break

    return all_deals


def main() -> dict:
    """
    Funcao principal: busca metadados de campos e etapas,
    depois busca deals de todos os pipelines configurados.
    """
    # 1. Buscar mapeamentos de IDs para nomes
    print("Buscando opcoes de campos (dealFields)...")
    options_map = fetch_deal_field_options()
    print(f"  -> {len(options_map)} campo(s) com opcoes carregado(s)")

    print("Buscando etapas (stages)...")
    stages_map = fetch_stages_map()
    print(f"  -> {len(stages_map)} etapa(s) carregada(s)")

    # 2. Buscar deals
    result: dict = {}
    total_deals = 0

    for pid in PIPELINE_IDS:
        print(f"Buscando deals do pipeline {pid}...")
        deals = fetch_deals_from_pipeline(pid, options_map, stages_map)
        result[f"pipeline_{pid}"] = deals
        total_deals += len(deals)
        print(f"  -> {len(deals)} deal(s) encontrado(s)")

    # Metadata
    br_tz = timezone(timedelta(hours=-3))
    result["metadata"] = {
        "total_deals": total_deals,
        "pipelines_consultados": PIPELINE_IDS,
        "fetched_at": datetime.now(br_tz).isoformat(),
    }

    # Salva em .tmp/
    tmp_dir = os.path.join(PROJECT_ROOT, ".tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    output_path = os.path.join(tmp_dir, "deals_pipedrive.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\nTotal de deals: {total_deals}")
    print(f"Resultado salvo em: {output_path}")

    return result


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    output = main()
    # Imprime JSON completo no stdout
    print("\n" + json.dumps(output, ensure_ascii=False, indent=2))
