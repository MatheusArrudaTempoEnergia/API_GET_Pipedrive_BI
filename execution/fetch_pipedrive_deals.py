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
import re
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
    "1325319e5d2ac98f8aede28d200773fa867dae18": "Cidade da Instalacao",
    "stage_change_time": "Data da Ultima Alteracao da Etapa",
    "6aa88ec119316d071ba4d6f48fcdd921877b0baf": "Data de Assinatura",
    "add_time": "Negocio Criado Em",
}

# Campos de data que devem ser formatados para dd/mm/yyyy
DATE_FIELDS = [
    "6aa88ec119316d071ba4d6f48fcdd921877b0baf",  # Data de Assinatura
    "bc6e85bfd61a7f514b65fbf8d1f3a7bacefc7f56",  # Data de Alteracao de Funil
    "add_time",                                    # Negocio Criado Em
]

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


def fetch_stages_map() -> tuple[dict[int, str], dict[int, int]]:
    """
    Busca as etapas (stages) de todos os pipelines configurados.

    Retorna:
        stages_map:    { stage_id: stage_name, ... }
        stages_order:  { stage_id: posicao_ordinal, ... }  (1-based)
    """
    stages_map: dict[int, str] = {}
    stages_order: dict[int, int] = {}

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

        # Ordena por order_nr para determinar a posicao correta
        stages_raw = sorted(
            (data.get("data") or []),
            key=lambda s: s.get("order_nr", 0),
        )
        for position, stage in enumerate(stages_raw, start=1):
            stages_map[stage["id"]] = stage["name"]
            stages_order[stage["id"]] = position

    return stages_map, stages_order


def fetch_pipelines_map() -> dict[int, str]:
    """
    Busca todos os pipelines da conta via API e retorna um dicionario
    { pipeline_id: pipeline_name }.
    """
    pipelines_map: dict[int, str] = {}
    url = f"{BASE_URL}/pipelines"
    params = {"api_token": API_TOKEN}
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if not data.get("success"):
        print("AVISO: Nao foi possivel buscar pipelines")
        return pipelines_map

    for pipeline in (data.get("data") or []):
        pipelines_map[pipeline["id"]] = pipeline["name"]

    return pipelines_map


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

    # Campos de data — formatar para dd/mm/yyyy
    if api_key in DATE_FIELDS:
        try:
            date_str = str(raw_value).strip()
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime("%d/%m/%Y")
                except ValueError:
                    continue
            return raw_value
        except Exception:
            return raw_value

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
# Funcao auxiliar — extracao do tipo de finder
# ---------------------------------------------------------------------------

def _extract_finder_type(finder_value: str | None) -> str | None:
    """
    Extrai o tipo do finder a partir do campo "Finder (Origem da Fatura)".

    Padrao esperado: "XX - Tipo - Nome"
    Exemplos:
        "01 - Interno - Jessica Silva"                             -> "Interno"
        "04 - Gold - JOSE VICTOR"                                  -> "Gold"
        "01 - Interno - Jessica Silva, 04 - Gold - JOSE VICTOR"   -> "Gold"
        "01 - Interno - PaP - Antonio"                             -> "Interno"

    Regra: se houver multiplos finders e um deles for "Interno",
    retorna o tipo do outro finder (priorizando o parceiro externo).
    """
    if not finder_value:
        return None

    # Separa finders individuais.
    # Cada finder começa com "digitos - ", entao splitamos na virgula
    # seguida de um novo padrao de finder.
    finders = re.split(r',\s*(?=\d+\s*-\s*)', finder_value)

    tipos: list[str] = []
    for finder in finders:
        finder = finder.strip()
        # Extrai o segundo segmento: "XX - TIPO - resto..." -> TIPO
        match = re.match(r'\d+\s*-\s*([^-]+)', finder)
        if match:
            tipos.append(match.group(1).strip())

    if not tipos:
        return None

    # Se ha apenas um tipo, retorna diretamente
    if len(tipos) == 1:
        return tipos[0]

    # Se ha multiplos, prioriza o tipo que NAO e "Interno"
    nao_interno = [t for t in tipos if t != "Interno"]
    if nao_interno:
        return nao_interno[0]

    # Todos sao "Interno" (improvavel, mas seguro)
    return tipos[0]


# Constante de tarifa para calculo de fatura
TARIFA_KWH = 1.1150643


def _extract_plan_percentage(tipo_plano: str | None) -> float | None:
    """
    Extrai o percentual do plano a partir do campo "Tipo do Plano".

    Exemplos:
        "Plano 25%"  -> 0.25
        "Plano 20%"  -> 0.20
        None         -> None
    """
    if not tipo_plano:
        return None
    match = re.search(r'(\d+)\s*%', tipo_plano)
    if match:
        return int(match.group(1)) / 100
    return None


# ---------------------------------------------------------------------------
# Funcao principal — busca de deals
# ---------------------------------------------------------------------------

def fetch_all_deals(
    options_map: dict[str, dict[int, str]],
    stages_map: dict[int, str],
    stages_order: dict[int, int],
    pipelines_map: dict[int, str],
) -> list[dict]:
    """
    Busca TODOS os deals da conta ({BASE_URL}/deals), tratando paginacao automatica.
    Resolve IDs de campos enum/set/stage para nomes legiveis.
    Retorna lista de todos os deals com os campos mapeados, incluindo um "pipeline_id"
    para posterior separacao.
    """
    all_deals: list[dict] = []
    start = 0

    while True:
        url = f"{BASE_URL}/deals"
        params = {
            "api_token": API_TOKEN,
            "start": start,
            "limit": PAGE_LIMIT,
            "status": "all_not_deleted",
            "everyone": 1,       # Buscar deals de todos os usuarios
        }

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()

        if not data.get("success"):
            print(f"AVISO: API retornou success=false ao buscar deals (start={start})")
            break

        deals_raw = data.get("data") or []
        if not deals_raw:
            break

        # Extrai e resolve os campos mapeados de cada deal
        for deal in deals_raw:
            raw_stage_id = deal.get("stage_id")
            mapped_deal = {
                "id": deal.get("id"),
                "pipeline_id": deal.get("pipeline_id")
            }
            for api_key, friendly_name in FIELD_MAP.items():
                raw_value = deal.get(api_key)
                mapped_deal[friendly_name] = resolve_field_value(
                    api_key, raw_value, options_map, stages_map
                )
            # Colocacao da Etapa — posicao ordinal dentro do pipeline
            if raw_stage_id is not None:
                mapped_deal["Colocacao da Etapa"] = stages_order.get(
                    int(raw_stage_id), None
                )
            else:
                mapped_deal["Colocacao da Etapa"] = None

            # Data de Criacao ou Alteracao do Funil:
            # Se "Data de Alteracao de Funil" for null, usa "Negocio Criado Em"
            data_funil = mapped_deal.get("Data de Alteracao de Funil")
            data_criacao = mapped_deal.get("Negocio Criado Em")
            mapped_deal["Data de Criacao ou Alteracao do Funil"] = (
                data_funil if data_funil is not None else data_criacao
            )

            # Tipo de Finder (Tratamento Interno):
            # Extrai o tipo (segundo segmento entre tracos) de cada finder.
            # Se houver multiplos finders e um deles for "Interno",
            # retorna o tipo do outro finder (ex: Gold, Plus).
            mapped_deal["Tipo de Finder (Tratamento Interno)"] = _extract_finder_type(
                mapped_deal.get("Finder (Origem da Fatura)")
            )

            # Fatura Cheia (R$): Media de Consumo (KWh) * TARIFA_KWH
            media_consumo = mapped_deal.get("Media de Consumo (KWh)")
            if media_consumo is not None:
                mapped_deal["Fatura Cheia (R$)"] = round(
                    float(media_consumo) * TARIFA_KWH, 2
                )
            else:
                mapped_deal["Fatura Cheia (R$)"] = None

            # Valor de Assinatura (R$):
            # kWh Contratado * TARIFA_KWH * (1 - percentual_do_plano)
            kwh_contratado = mapped_deal.get("kWh Contratado")
            percentual = _extract_plan_percentage(
                mapped_deal.get("Tipo do Plano")
            )
            if kwh_contratado is not None and percentual is not None:
                mapped_deal["Valor de Assinatura (R$)"] = round(
                    float(kwh_contratado) * TARIFA_KWH * (1 - percentual), 2
                )
            else:
                mapped_deal["Valor de Assinatura (R$)"] = None

            # Custo Nao Compensavel: kWh Nao Compensavel * TARIFA_KWH
            kwh_nao_compensavel = mapped_deal.get("kWh Nao Compensavel")
            if kwh_nao_compensavel is not None:
                mapped_deal["Custo Nao Compensavel"] = round(
                    float(kwh_nao_compensavel) * TARIFA_KWH, 2
                )
            else:
                mapped_deal["Custo Nao Compensavel"] = None

            # Funil: nome do pipeline ao qual o deal pertence
            raw_pipeline_id = deal.get("pipeline_id")
            if raw_pipeline_id is not None:
                mapped_deal["Funil"] = pipelines_map.get(
                    int(raw_pipeline_id), str(raw_pipeline_id)
                )
            else:
                mapped_deal["Funil"] = None

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
    stages_map, stages_order = fetch_stages_map()
    print(f"  -> {len(stages_map)} etapa(s) carregada(s)")

    print("Buscando pipelines...")
    pipelines_map = fetch_pipelines_map()
    print(f"  -> {len(pipelines_map)} pipeline(s) carregado(s)")

    # 2. Buscar deals
    print("Buscando todos os deals da conta...")
    all_deals = fetch_all_deals(options_map, stages_map, stages_order, pipelines_map)
    print(f"  -> {len(all_deals)} deal(s) encontrado(s) no total")

    result: dict = {}
    total_deals = 0

    for pid in PIPELINE_IDS:
        print(f"Filtrando deals do pipeline {pid}...")
        deals_for_pid = [d for d in all_deals if d.get("pipeline_id") == pid]
        
        # Remove a chave pipeline_id interna para manter a estrutura original
        for d in deals_for_pid:
            d.pop("pipeline_id", None)

        result[f"pipeline_{pid}"] = deals_for_pid
        total_deals += len(deals_for_pid)
        print(f"  -> {len(deals_for_pid)} deal(s) alocado(s)")

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
