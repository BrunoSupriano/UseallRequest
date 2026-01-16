# %%
# IMPORTS E ENV
# =========================
import os
import json
import time
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(override=True)

BASE_URL = os.getenv("USEALL_BASE_URL")
TOKEN = os.getenv("USEALL_TOKEN")
DB_URL = os.getenv("DB_URL")
SCHEMA = os.getenv("DB_SCHEMA")

IDENTIFICACAO = "m2_estoque_custos"
DATA_REF = "15/01/2026"
ESPERA = 185


HEADERS = {
    "accept": "application/json",
    "use-relatorio-token": os.getenv("USEALL_TOKEN")
}


# %%
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# %%
# PATHS
# =========================
RAW_DIR = "data/raw"
RAW_FINAL = "data/staging_custos_raw.json"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs("data", exist_ok=True)


# %%
# GRUPOS
# =========================
grupos = {
    "setup_automacao": [333],
    "setup": [336,387,520,404,558,578,341,390],
    "lojas": [335,334],
    "vm": [345,344,346],
    "ctfm": [342,343],
    "servicos": [339,340,381,389]
}


# %%
# TESTE VARIÁVEIS DOTENV
# =========================
env_vars = {
    "BASE_URL": BASE_URL,
    "TOKEN": TOKEN,
    "DB_URL": DB_URL,
    "SCHEMA": SCHEMA,
}

for key, value in env_vars.items():
    if value:
        logging.info(f"{key} carregada: {value}")
    else:
        logging.error(f"{key} não carregada ou vazia")

# %%
# EXTRAÇÃO POR GRUPO
# =========================
todos_registros = []

for nome, ids in grupos.items():
    logging.info(f"Iniciando grupo: {nome}")

    filtros = [
        {"Nome": "idfilial", "Valor": ids, "Operador": 1},
        {"Nome": "FILTROSREGISTROSATIVO", "Valor": ""},
        # {"Nome": "FILTROSREGISTROSATIVO", "Valor": " AND IA.ATIVO = 1 AND I.ATIVO = 1"}, # para considerar apenas itens ativos e almox ativos
        {
            "Nome": "filtroswhere",
            "Valor": f" AND IDFILIAL IN ({','.join(map(str, ids))})"
            #"Valor": f" AND (SALDO > 0) AND IDFILIAL IN ({','.join(map(str, ids))})" # para considerar apenas itens com saldo
        },
        {"Nome": "data", "Valor": DATA_REF}
    ]

    params = {
        "Identificacao": IDENTIFICACAO,
        "FiltrosSqlQuery": json.dumps(filtros, ensure_ascii=False)
    }

    while True:
        r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=180)

        if r.status_code == 200:
            payload = r.json()

            path = f"{RAW_DIR}/{nome}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)

            registros = payload.get("data") if isinstance(payload, dict) else payload
            if registros:
                for row in registros:
                    row["_grupo_origem"] = nome
                    todos_registros.append(row)

            logging.info(
                f"Grupo '{nome}' salvo ({len(registros) if registros else 0} registros)"
            )
            break

        if r.status_code == 429:
            logging.warning(f"429 Rate limit | {nome} | aguardando {ESPERA}s")
            time.sleep(ESPERA)
            continue

        if r.status_code == 400:
            logging.error(f"400 Payload pesado | Grupo {nome}")
            break

        r.raise_for_status()

    logging.info(f"Aguardando {ESPERA}s\n")
    time.sleep(ESPERA)


# %%
# CONSOLIDA STAGING_CUSTOS_RAW
# =========================
with open(RAW_FINAL, "w", encoding="utf-8") as f:
    json.dump(todos_registros, f, ensure_ascii=False)

logging.info(
    f"Arquivo consolidado criado: {RAW_FINAL} ({len(todos_registros)} registros)"
)


# %%
# CARGA NO POSTGRES
# =========================
engine = create_engine(DB_URL)

df = pd.DataFrame(todos_registros)

df.to_sql(
    "staging_custos",
    engine,
    schema=SCHEMA,
    if_exists="append",
    index=False,
    chunksize=1000,
    method="multi"
)

logging.info("Carga no PostgreSQL finalizada")



