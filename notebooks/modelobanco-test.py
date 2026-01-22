# %% [markdown]
#   # Extração de API Useall

# %% [markdown]
#   ## Tools and functions

# %%
import os
import requests
import json
import time
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

BASE_URL = os.getenv("USEALL_BASE_URL")

HEADERS = {
    "accept": "application/json",
    "use-relatorio-token": os.getenv("USEALL_TOKEN")
}

def buscar_dados_api(identificacao, nome_arquivo, backend_filters=None, extra_params=None):
    """Busca dados na API UseAll e retorna um DataFrame (ou None em caso de erro/vazio)"""
    
    query_params = {"Identificacao": identificacao}
    
    if backend_filters:
        query_params["FiltrosSqlQuery"] = json.dumps(backend_filters, ensure_ascii=False)
        
    if extra_params:
        query_params.update(extra_params)

    print(f"[{time.strftime('%H:%M:%S')}] Iniciando extração: {nome_arquivo}...")
    
    while True:
        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=query_params, timeout=500)
            
            if response.status_code == 429:
                print(f"[{time.strftime('%H:%M:%S')}] Erro 429 (Too Many Requests) em {nome_arquivo}. Aguardando 185 segundos...")
                time.sleep(185)
                continue
                
            response.raise_for_status()

            data = response.json()
            df = pd.DataFrame(data)
            return df

        except requests.exceptions.Timeout:
            print(f"[{time.strftime('%H:%M:%S')}] Timeout atingido para {nome_arquivo}. Aguardando 185 segundos...")
            time.sleep(185)
            continue
            
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Erro irrecuperável em {nome_arquivo}: {str(e)}")
            return None

def salvar_parquet(df, nome_arquivo):
    """Salva o DataFrame em arquivo parquet"""
    if df is not None and not df.empty:
        # Garante extensão .parquet
        if not nome_arquivo.endswith('.parquet'):
            nome_arquivo += '.parquet'
        
        try:
            df.to_parquet(nome_arquivo, index=False)
            print(f"[{time.strftime('%H:%M:%S')}] Sucesso ao salvar: {nome_arquivo} (Linhas: {len(df)})")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Erro ao salvar {nome_arquivo}: {str(e)}")
    else:
        print(f"[{time.strftime('%H:%M:%S')}] Nada a salvar para {nome_arquivo} (DataFrame vazio ou None)")

def verificar_tipos_dados():
    print("\n" + "=" * 40)
    print(f"[{time.strftime('%H:%M:%S')}] VERIFICAÇÃO DE TIPOS DE DADOS")
    print("=" * 40)

    encontrou = False

    for nome, obj in globals().items():
        if isinstance(obj, pd.DataFrame):
            encontrou = True
            print(f"\nDataFrame: {nome}")
            if not obj.empty:
                print("-" * 30)
                print(obj.dtypes)
                print("-" * 30)
            else:
                print("  (DataFrame vazio)")

    if not encontrou:
        print("Nenhum DataFrame encontrado em memória.")

# --- Defines Auxiliares de Filtro ---
def filtro_simples(nome, valor):
    return {"Nome": nome, "Valor": valor, "Operador": None, "Descricao": None, "ValorFormatado": None}

def carregar_dfs_globais(tarefas):
    print(f"[{time.strftime('%H:%M:%S')}] --- INICIANDO CARGA EM MEMÓRIA ---")

    for t in tarefas:
        nome = t["nome"]
        df = buscar_dados_api(
            t["id"],
            nome,
            t.get("filtros"),
            t.get("extra_params")
        )

        if df is not None:
            globals()[nome] = df
        else:
            print(f"[{time.strftime('%H:%M:%S')}] Falha ao carregar {nome}")

def carregar_tarefa_complexa(tarefa):
    nome = tarefa["nome"]

    df = buscar_dados_api(
        tarefa["id"],
        nome,
        tarefa.get("filtros"),
        tarefa.get("extra_params")
    )

    if df is not None:
        globals()[nome] = df
    else:
        print(f"[{time.strftime('%H:%M:%S')}] Falha ao carregar {nome}")


pipeline_start = time.time()
print(f"--- Pipeline iniciada em {time.strftime('%d/%m/%Y %H:%M:%S')} ---")




# %% [markdown]
#   ## Variaveis de filtros

# %% [markdown]
#   ### Simples

# %%
params_fixos = {"pagina": 1, "qtderegistros": 1}

tarefas_simples = [
    {
        "nome": "dfuseallitens",
        "id": "m2_estoque_item",
        "filtros": [
            filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"),
            filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallunidades",
        "id": "m2_estoque_unidade",
        "filtros": [
            filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"),
            filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallsegmentos",
        "id": "m2_vendas_segmento",
        "filtros": [
            filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"),
            filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallcidades",
        "id": "m2_geral_cidades",
        "filtros": [
            filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"),
            filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallsolcompra",
        "id": "m2_compras_m2_compras_solicitacao_de_compras__extra",
        "filtros": [
            filtro_simples("DATAINI", "01/01/1900"),
            filtro_simples("DataFim", "01/01/2500")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallfiliais",
        "id": "m2_geral_filiais",
        "filtros": [
            filtro_simples("DATAHORAALTINI", "01/01/1900, 11:00:00"),
            filtro_simples("DATAHORAALTFIM", "01/01/2500, 14:00:00")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallempresas",
        "id": "m2_geral_empresas",
        "filtros": [
            filtro_simples("DATAHORAALTINI", "01/01/2022, 11:00:00"),
            filtro_simples("DATAHORAALTFIM", "01/01/2027, 14:00:00")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallexpedição",
        "id": "m2_vendas_extracao_de_dados__saida_expedicao",
        "filtros": [
            filtro_simples("data1", "01/01/1900"),
            filtro_simples("data2", "01/01/2500")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallclientesfornecedore",
        "id": "m2_geral_clientes__fornecedores",
        "filtros": [
            filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"),
            filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")
        ],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallalmoxarifados",
        "id": "m2_estoque_almoxarifados",
        "filtros": [
            filtro_simples("DATAHORAALTINI", "01/01/1900"),
            filtro_simples("DATAHORAALTFIM", "01/01/2500")
        ],
        "extra_params": params_fixos
    },
]




# %% [markdown]
#   ### COMPLEXAS

# %%
filtros_req = [
    {
        "Nome": "IDFILIAL",
        "Valor": [
            333,
            339,
            340,
            381,
            389,
            336,
            387,
            520,
            404,
            558,
            578,
            341,
            390,
            345,
            344,
            346,
            335,
            334,
            342,
            343,
        ],
        "Operador": 1,
    },
    {
        "Nome": "DATA",
        "Valor": "01/01/2010,01/01/2027",
        "Operador": 8,
        "TipoPeriodoData": 5,
    },
    {
        "Nome": "DATAPREVATEND",
        "Valor": "01/01/2010,01/01/2027",
        "Operador": 8,
        "TipoPeriodoData": 8,
    },
    {"Nome": "CLASSGRUPOITEM", "Valor": ""},
    {"Nome": "CLASSCONTACDC", "Valor": ""},
    {"Nome": "quebra", "Valor": 1},
    {"Nome": "FILTROSWHERE", "Valor": " AND IDEMPRESA = 211"},
]

filtros_estoque = [
    {"Nome": "ADDATA", "Valor": "22/01/2026"},
    {"Nome": "ANQUEBRA", "Valor": 0},
    {"Nome": "FILTROSWHEREFORNEC", "Valor": ""},
    {
        "Nome": "FILTROSREGISTROSATIVO",
        "Valor": " AND ITEM.ATIVO = 1 AND ALMOX.ATIVO = 1 AND ITEMALMOX.ATIVO = 1",
    },
    {
        "Nome": "FILTROSWHERE",
        "Valor": " AND EXISTS(SELECT 1 FROM USE_USUARIOS_FILIAIS UFILIAIS "
        "WHERE UFILIAIS.IDEMPRESA = T.IDEMPRESA "
        "AND UFILIAIS.IDFILIAL = T.IDFILIAL "
        "AND UFILIAIS.IDUSUARIO = 7332) "
        "AND T.IDFILIAL in (333,339, 340, 381, 389, 336, 387, 520, 404, 558, 578, 341, 390, 345, 344, 346, 335, 334, 342, 343)",
    },
]


"""    "Parametros": [
        {
            "Nome": "filter",
            "Valor": "Filial: LOJA - ARARANGUA\nData: 22/01/2026\nIncluir apenas itens e almoxarifados ativos: Sim",
        },
        {"Nome": "quebra", "Valor": 0},
        {"Nome": "ordenacao", "Valor": 0},
    ],
    
    "Identificacao": "m2_estoque_saldo_de_estoque",
    "FiltrosSql": [
        {"Nome": "ADDATA", "Valor": "22/01/2026"},
        {"Nome": "ANQUEBRA", "Valor": 0},
        {"Nome": "FILTROSWHEREFORNEC", "Valor": ""},
        {
            "Nome": "FILTROSREGISTROSATIVO",
            "Valor": " AND ITEM.ATIVO = 1 AND ALMOX.ATIVO = 1 AND ITEMALMOX.ATIVO = 1",
        },
"""


filtros_atend = [
    {
        "Nome": "FILTROSWHERE",
        "Valor": (
            "WHERE IDEMPRESA = 211 "
            "AND IDFILIAL IN (333,339, 340, 381, 389, 336, 387, 520, 404, 558, 578, 341, 390, 345, 344, 346, 335, 334, 342, 343) "
            "AND DATA_REQ BETWEEN '01/01/1900' AND '01/01/2900' "
            "AND DATA_ATEND BETWEEN '01/01/1900' AND '01/01/2900'"
        ),
    }
]

params_atend = {
    "NomeOrganizacao": "SETUP SERVICOS ESPECIALIZADOS LTDA",
    "Parametros": json.dumps(
        [{"Nome": "usecellmerging", "Valor": True}, {"Nome": "quebra", "Valor": 0}]
    ),
}

# ===============================
# BLOCO 1 — REQUISIÇÕES
# ===============================

tarefa_requisicoes = {
    "nome": "dfuseallrequisicoes",
    "id": "m2_estoque_requisicao_de_materiais",
    "filtros": filtros_req,
    "extra_params": None,
}


# ===============================
# BLOCO 2 — ESTOQUE
# ===============================

tarefa_estoque = {
    "nome": "dfuseallestoque",
    "id": "m2_estoque_saldo_de_estoque",
    "filtros": filtros_estoque,
    "extra_params": None,
}

# ===============================
# BLOCO 3 — ATENDIMENTO DE REQUISIÇÕES
# ===============================

tarefa_atendimento = {
    "nome": "dfuseallatendimentodereq",
    "id": "m2_estoque_atendimentos_de_requisicao",
    "filtros": filtros_atend,
    "extra_params": params_atend,
}

# %% [markdown]
#   ## Criando DataFrames

# %% [markdown]
#   ### Usando funções

# %%
carregar_dfs_globais(tarefas_simples)



# %%
carregar_tarefa_complexa(tarefa_estoque)



# %%
carregar_tarefa_complexa(tarefa_atendimento)



# %%
carregar_tarefa_complexa(tarefa_requisicoes)

# %% [markdown]
#   ### Api Custos - Particularidade de loop

# %%
# %%
# IMPORTS E ENV
import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv


load_dotenv(override=True)

BASE_URL = os.getenv("USEALL_BASE_URL")
TOKEN = os.getenv("USEALL_TOKEN")

IDENTIFICACAO = "m2_estoque_custos"
DATA_REF = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y")
ESPERA = 185

HEADERS = {
    "accept": "application/json",
    "use-relatorio-token": TOKEN
}

# %%
# LOGGING (Jupyter-safe)
logger = logging.getLogger("useall_pipeline")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# %%
# PATHS
RAW_DIR = "data/raw"
RAW_FINAL = "data/staging_custos_raw.parquet"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs("data", exist_ok=True)

# %%
# FUNÇÕES AUXILIARES
def ja_baixado_hoje(path: str) -> bool:
    if not os.path.exists(path):
        return False
    mod_time = datetime.fromtimestamp(os.path.getmtime(path))
    return mod_time.date() == datetime.today().date()


def log_etapa(msg: str, inicio: float | None = None) -> float:
    agora = time.time()
    if inicio:
        logger.info(f"{msg} | duração: {agora - inicio:.2f}s")
    else:
        logger.info(msg)
    return agora

# %%
# GRUPOS
grupos = {
    "ctfm": [342, 343],
    "lojas": [335, 334],
    "vm": [345, 344, 346],
    "setup_automacao": [333],
    "servicos": [339, 340, 381, 389],
    "setup": [336, 387, 520, 404, 558, 578, 341, 390],
}


# %%
# PIPELINE
pipeline_start = log_etapa("INÍCIO DO PIPELINE")

if ja_baixado_hoje(RAW_FINAL):
    logger.info("Raw consolidado já existe e é de hoje. Pipeline encerrado.")
else:
    logger.info("Raw do dia não encontrado. Iniciando download por grupos.")

    nomes_grupos = list(grupos.items())
    total_grupos = len(nomes_grupos)

    for idx, (nome, ids) in enumerate(nomes_grupos, start=1):
        ultimo_grupo = idx == total_grupos
        grupo_start = log_etapa(f"INÍCIO GRUPO {idx}/{total_grupos}: {nome}")

        raw_path = f"{RAW_DIR}/{nome}.parquet"

        if ja_baixado_hoje(raw_path):
            logger.info(f"{nome} já existe e é de hoje. Pulando grupo.")
            continue

        filtros = [
            {"Nome": "idfilial", "Valor": ids, "Operador": 1},
            {"Nome": "FILTROSREGISTROSATIVO", "Valor": ""},
            {"Nome": "filtroswhere", "Valor": f" AND IDFILIAL IN ({','.join(map(str, ids))})"},
            {"Nome": "data", "Valor": DATA_REF}
        ]

        params = {
            "Identificacao": IDENTIFICACAO,
            "FiltrosSqlQuery": json.dumps(filtros, ensure_ascii=False)
        }

        request_start = log_etapa(f"Requisição API | Grupo {nome}")

        while True:
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=180)

            if r.status_code == 200:
                log_etapa(f"Resposta API OK | Grupo {nome}", request_start)

                payload = r.json()
                registros = payload.get("data") if isinstance(payload, dict) else payload

                if registros:
                    for row in registros:
                        row["_grupo_origem"] = nome

                    df = pd.DataFrame(registros)

                    save_start = time.time()
                    df.to_parquet(raw_path, engine="pyarrow", index=False)
                    log_etapa(
                        f"Arquivo salvo | {raw_path} | registros: {len(df)}",
                        save_start
                    )
                else:
                    logger.warning(f"Grupo {nome} retornou 0 registros")

                break

            if r.status_code == 429:
                logger.warning(f"429 Rate limit | {nome} | aguardando {ESPERA}s")
                time.sleep(ESPERA)
                continue

            if r.status_code == 400:
                logger.error(f"400 Payload pesado | Grupo {nome}")
                break

            r.raise_for_status()

        log_etapa(f"FIM GRUPO: {nome}", grupo_start)

        # espera somente se NÃO for o último grupo
        if not ultimo_grupo:
            logger.info(f"Cooldown {ESPERA}s antes do próximo grupo")
            time.sleep(ESPERA)

log_etapa("FIM DO PIPELINE", pipeline_start)

# %%
todos_registros = []

for arquivo in os.listdir(RAW_DIR):
    if arquivo.endswith(".parquet"):
        path = os.path.join(RAW_DIR, arquivo)

        df = pd.read_parquet(path, engine="pyarrow")

        # origem = nome do arquivo sem extensão
        grupo_origem = os.path.splitext(arquivo)[0]
        df["_grupo_origem"] = grupo_origem

        todos_registros.append(df)
        logging.info(f"Lido {arquivo} ({len(df)} registros)")

if todos_registros:
    df_final = pd.concat(todos_registros, ignore_index=True)
    df_final.to_parquet(RAW_FINAL, engine="pyarrow", index=False)
    logging.info(
        f"Arquivo consolidado criado: {RAW_FINAL} ({len(df_final)} registros)"
    )
else:
    logging.info("Nenhum arquivo Parquet encontrado em RAW_DIR.")

# %% [markdown]
#   ### Verificando Tipos

# %%
# --- 3. Verificação de Tipos ---
print(f"[{time.strftime('%H:%M:%S')}] --- INICIANDO VERIFICAÇÃO DE TIPOS ---")
verificar_tipos_dados()

# %% [markdown]
#   ## Configurações Banco de Dados

# %%
# Carregando .env
import io
import os
import time
from urllib.parse import quote
from sqlalchemy import create_engine, text

user = quote(os.getenv("PG_USER"))
password = quote(os.getenv("PG_PASSWORD"))
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
dbname = os.getenv("PG_DBNAME")

SCHEMA = os.getenv("DB_SCHEMA")

DB_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(DB_URL)

# ---------------------------------------

# garante schema
with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
    conn.commit()


# %% [markdown]
#   ## Staging - Bronze - Dados Brutos tipos indefinidos

# %%
# custos - particularidade staging
import io
import pandas as pd
from sqlalchemy import text

parquet_file = "data/staging_custos_raw.parquet"
table_name = "staging_custos"

df = pd.read_parquet(parquet_file, engine="pyarrow")

if not df.empty:
    csv_buffer = io.StringIO()
    df.to_csv(
        csv_buffer,
        index=False,
        header=False,
        sep=",",
        quotechar='"',
        quoting=1,        # csv.QUOTE_ALL
        escapechar="\\"
    )
    csv_buffer.seek(0)

    cols_with_types = ", ".join([f'"{col}" TEXT' for col in df.columns])

    # garante tabela + limpa dados (FAST)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{table_name}"))
        conn.execute(text(f"""
            CREATE TABLE {SCHEMA}.{table_name} (
                {cols_with_types}
            )
        """))

    # COPY ultra-rápido
    raw_conn = engine.raw_connection()
    cursor = raw_conn.cursor()
    cursor.copy_expert(
        f"""
        COPY {SCHEMA}.{table_name}
        FROM STDIN
        WITH (
            FORMAT CSV,
            QUOTE '"',
            ESCAPE '\\'
        )
        """,
        csv_buffer
    )
    raw_conn.commit()
    cursor.close()
    raw_conn.close()

    print(f"Tabela {SCHEMA}.{table_name} substituída via COPY.")
else:
    print("Parquet vazio.")


# %%
ordem_staging = [
    # simples iniciais
    "dfuseallitens",
    "dfuseallunidades",
    "dfuseallsegmentos",
    "dfuseallcidades",

    # complexas no meio
    "dfuseallrequisicoes",
    "dfuseallestoque",
    "dfuseallatendimentodereq",

    # simples finais
    "dfuseallsolcompra",
    "dfuseallfiliais",
    "dfuseallempresas",
    "dfuseallexpedição",
    "dfuseallclientesfornecedore",
    "dfuseallalmoxarifados"
]




# %%
def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def copy_df_to_postgres(df, schema: str, table: str):
    import psycopg2
    import io

    buffer = io.StringIO()
    df.to_csv(
        buffer,
        index=False,
        header=False,
        sep="\t",
        na_rep="\\N"
    )
    buffer.seek(0)
    conn = psycopg2.connect(
        dbname=os.getenv("PG_DBNAME"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT")
    )
    cur = conn.cursor()

    sql = f"""
        COPY {schema}.{table}
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER E'\t', NULL '\\N')
    """

    cur.copy_expert(sql, buffer)

    conn.commit()
    cur.close()
    conn.close()

tabelas_criadas = 0
dfs_nao_encontrados = []

log("INICIANDO CARGA STAGING (COPY FROM)")

for df_nome in ordem_staging:
    df = globals().get(df_nome)

    if df is None or df.empty:
        dfs_nao_encontrados.append(df_nome)
        continue

    tabela = "staging_" + df_nome.replace("dfuseall", "")
    tabela = tabela.lower()

    log(f"Preparando tabela {SCHEMA}.{tabela} | Linhas: {len(df)}")

    # 1️⃣ cria estrutura (DDL leve)
    with engine.connect() as conn:
        df.head(0).to_sql(
            name=tabela,
            con=conn,
            schema=SCHEMA,
            if_exists="replace",
            index=False
        )
        conn.commit()

    log(f"Iniciando COPY para {SCHEMA}.{tabela}")

    # 2️⃣ carga pesada via COPY
    copy_df_to_postgres(df, SCHEMA, tabela)

    log(f"[OK] Tabela {SCHEMA}.{tabela} carregada com sucesso")

    tabelas_criadas += 1


# ---------------- FINAL ----------------

log("--------------------------------------------------")

if tabelas_criadas == 0:
    log("Nenhuma tabela staging foi criada.")
    log("DataFrames não encontrados:")
    for nome in dfs_nao_encontrados:
        log(f" - {nome}")
else:
    log(f"{tabelas_criadas} tabelas staging criadas com sucesso.")

log("PROCESSO FINALIZADO")


# %% [markdown]
#   ## Silver definindo tipos automaticamente

# %%
import json
from pathlib import Path

SCHEMA_FILE = Path("schema_silver.json")

SCHEMA = "useall"
SAMPLE_LIMIT = 50000

import pandas as pd
import re


def load_or_create_schema(engine, schema, staging_tables):
    # CASO 1 — schema já existe
    if SCHEMA_FILE.exists():
        log("[SCHEMA] Usando schema_silver.json existente")
        with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    # CASO 2 — primeira execução → inferir
    log("[SCHEMA] schema_silver.json não encontrado. Inferindo tipos...")

    schema_silver = {}

    for staging_table in staging_tables:
        silver_table = silver_table_name(staging_table)
        log(f"Profiling {schema}.{staging_table} -> {schema}.{silver_table}")

        df_sample = pd.read_sql(
            f'SELECT * FROM {schema}."{staging_table}" LIMIT {SAMPLE_LIMIT}',
            engine
        )

        schema_silver[silver_table] = {
            "staging_table": staging_table,
            "columns": {
                col.lower(): {
                    **infer_column_type_final(df_sample[col]),
                    "source_col": col
                }
                for col in df_sample.columns
            }
        }

    # Salva schema congelado
    with open(SCHEMA_FILE, "w", encoding="utf-8") as f:
        json.dump(schema_silver, f, indent=2, ensure_ascii=False)

    log("[SCHEMA] schema_silver.json criado e congelado")
    return schema_silver


# Detecta formato de data
def is_date_series(s: pd.Series):
    sample = s.dropna().astype(str).head(50)
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            pd.to_datetime(sample, format=fmt)
            return fmt
        except:
            continue
    return None

# Inferência de tipo
def infer_column_type_final(series: pd.Series) -> dict:
    s = series.dropna()
    if s.empty:
        return {"type": "text"}

    # BOOLEAN lógico
    if s.astype(str).isin(["0","1","true","false","True","False"]).all():
        return {"type": "boolean"}

    # DATE / TIMESTAMP
    date_fmt = is_date_series(s)
    if date_fmt:
        return {"type": "timestamp", "format": date_fmt}

    # INTEGER
    if s.astype(str).str.fullmatch(r"-?\d+").all():
        return {"type": "bigint"}

    # DECIMAL
    if s.astype(str).str.fullmatch(r"-?\d+(\.\d+)?").all():
        return {"type": "numeric(18,4)"}

    return {"type": "text"}

from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_type = 'BASE TABLE'
          AND table_name LIKE 'staging_%'
    """), {"schema": SCHEMA})

    staging_tables = [row[0] for row in result.fetchall()]

# Função nome silver
def silver_table_name(staging_table: str) -> str:
    return staging_table.replace("staging_", "silver_")

schema_silver = load_or_create_schema(
    engine=engine,
    schema=SCHEMA,
    staging_tables=staging_tables
)


# Cria cast SQL
def generate_cast_sql(col_dest, meta):
    col_src = meta["source_col"]

    col_sql = f'"{col_src}"'
    col_txt = f'{col_sql}::text'

    if meta["type"] == "boolean":
        return f"""
        CASE
            WHEN lower({col_txt}) IN ('1','true','sim','s','y','yes') THEN true
            WHEN lower({col_txt}) IN ('0','false','nao','n','no') THEN false
            ELSE NULL
        END AS "{col_dest}"
        """

    if meta["type"] == "timestamp":
        fmt = meta.get("format")

        pg_fmt = {
            "%Y-%m-%d": "YYYY-MM-DD",
            "%Y-%m-%d %H:%M:%S": "YYYY-MM-DD HH24:MI:SS",
            "%Y-%m-%dT%H:%M:%S": "YYYY-MM-DD\"T\"HH24:MI:SS",
            "%d/%m/%Y": "DD/MM/YYYY",
            "%d/%m/%Y %H:%M:%S": "DD/MM/YYYY HH24:MI:SS",
        }.get(fmt)

        if pg_fmt:
            return f"""
            CASE
                WHEN {col_txt} = '' THEN NULL
                ELSE to_timestamp({col_txt}, '{pg_fmt}')
            END AS "{col_dest}"
            """
        else:
            return f"""
            CASE
                WHEN {col_sql} IS NULL OR {col_txt} = '' THEN NULL
                ELSE {col_sql}::timestamp
            END AS "{col_dest}"
            """


    if meta["type"] in ("bigint","numeric(18,4)"):
        return f"""
        CASE
            WHEN {col_txt} ~ '^-?\\d+(\\.\\d+)?$' THEN {col_txt}::{meta["type"]}
            ELSE NULL
        END AS "{col_dest}"
        """

    return f'{col_sql}::text AS "{col_dest}"'

# Gera CREATE TABLE
def generate_create_table(schema, table, columns: dict):
    cols = ",\n  ".join(f'"{col_dest}" {meta["type"]}' for col_dest, meta in columns.items())
    return f"""
    DROP TABLE IF EXISTS {schema}."{table}";
    CREATE TABLE {schema}."{table}" (
      {cols}
    );
    """

# Cria tabelas silver
for silver_table, meta in schema_silver.items():
    log(f"Criando tabela silver {SCHEMA}.{silver_table}")
    ddl = generate_create_table(SCHEMA, silver_table, meta["columns"])
    with engine.begin() as conn:
        conn.execute(text(ddl))

# Gera INSERT
def generate_insert_cast(staging_schema, SCHEMA, staging_table, silver_table, columns):
    selects = ",\n".join(generate_cast_sql(col_dest, meta) for col_dest, meta in columns.items())
    return f"""
    INSERT INTO {SCHEMA}."{silver_table}"
    SELECT
      {selects}
    FROM {staging_schema}."{staging_table}";
    """

# Carrega dados
for silver_table, meta in schema_silver.items():
    staging_table = meta["staging_table"]
    columns = meta["columns"]
    log(f"Carregando dados em {SCHEMA}.{silver_table}")
    sql = generate_insert_cast(SCHEMA, SCHEMA, staging_table, silver_table, columns)
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        log(f"[OK] {SCHEMA}.{silver_table} carregada")
    except Exception as e:
        log(f"[ERRO] {SCHEMA}.{silver_table} -> {e}")

log("--------------------------------------------------")
log("PROCESSO FINALIZADO")

# %% [markdown]
#   ## Gold - Adicionando novas colunas e agregando valor

# %%
from sqlalchemy import create_engine, text

engine = create_engine(DB_URL)

sql = """
DO $$
DECLARE
    r RECORD;
    gold_table TEXT;
BEGIN
    FOR r IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'useall'
          AND table_name LIKE 'silver_%'
    LOOP

        gold_table := replace(r.table_name, 'silver_', 'gold_');

        -- CASO ESPECIAL: SILVER_REQUISICOES
        IF r.table_name = 'silver_requisicoes' THEN

            -- cria se não existir
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS useall.%I AS
                 SELECT
                     *,
                     CASE status::int
                         WHEN 0  THEN ''Digitado''
                         WHEN 1  THEN ''Aberto''
                         WHEN 3  THEN ''Cancelado''
                         WHEN 10 THEN ''Parcial''
                         WHEN 11 THEN ''Atendido''
                         ELSE ''Desconhecido''
                     END AS py_desc_status
                 FROM useall.silver_requisicoes
                 WHERE false;',
                gold_table
            );

            -- limpa e reinsere
            EXECUTE format('TRUNCATE TABLE useall.%I;', gold_table);

            EXECUTE format(
                'INSERT INTO useall.%I
                 SELECT
                     *,
                     CASE status::int
                         WHEN 0  THEN ''Digitado''
                         WHEN 1  THEN ''Aberto''
                         WHEN 3  THEN ''Cancelado''
                         WHEN 10 THEN ''Parcial''
                         WHEN 11 THEN ''Atendido''
                         ELSE ''Desconhecido''
                     END AS py_desc_status
                 FROM useall.silver_requisicoes;',
                gold_table
            );

        -- DEMAIS TABELAS
        ELSE

            -- cria se não existir
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS useall.%I AS
                 SELECT * FROM useall.%I WHERE false;',
                gold_table,
                r.table_name
            );

            -- limpa e reinsere
            EXECUTE format('TRUNCATE TABLE useall.%I;', gold_table);

            EXECUTE format(
                'INSERT INTO useall.%I
                 SELECT * FROM useall.%I;',
                gold_table,
                r.table_name
            );

        END IF;

    END LOOP;
END $$;
"""

with engine.begin() as conn:
    conn.execute(text(sql))

# %% [markdown]
#   ## Dim_Calendario

# %%
from sqlalchemy import create_engine, text

# ---------------- SQL ----------------
sql_create_dim_calendario = text("""
CREATE TABLE IF NOT EXISTS useall.dim_calendario (
    data DATE PRIMARY KEY,

    ano INT,
    mes INT,
    dia INT,

    ano_mes TEXT,
    ano_mes_atual TEXT,
    ano_mes_ordem INT,

    nome_mes TEXT,
    nome_mes_abrev TEXT,
    nome_dia TEXT,
    nome_dia_abrev TEXT,

    dia_semana INT,
    semana_iso INT,
    ano_iso INT,
    trimestre INT,
    
    dia_mes_abr TEXT,

    is_fim_de_semana BOOLEAN,
    is_feriado BOOLEAN,
    nome_feriado TEXT
);
""")

sql_create_indices = text("""
CREATE INDEX IF NOT EXISTS idx_dim_calendario_data
    ON useall.dim_calendario (data);

CREATE INDEX IF NOT EXISTS idx_dim_calendario_ano_mes_ordem
    ON useall.dim_calendario (ano_mes_ordem);
""")

sql_atualiza_calendario = text("""
INSERT INTO useall.dim_calendario (
    data,
    ano,
    mes,
    dia,
    ano_mes,
    ano_mes_atual,
    ano_mes_ordem,
    nome_mes,
    nome_mes_abrev,
    nome_dia,
    nome_dia_abrev,
    dia_semana,
    semana_iso,
    ano_iso,
    trimestre,
    dia_mes_abr,
    is_fim_de_semana,
    is_feriado,
    nome_feriado
)
SELECT DISTINCT
    d::date AS data,

    EXTRACT(YEAR FROM d)::int AS ano,
    EXTRACT(MONTH FROM d)::int AS mes,
    EXTRACT(DAY FROM d)::int AS dia,

    TO_CHAR(d, 'YYYY/MM') AS ano_mes,

    CASE
        WHEN EXTRACT(YEAR FROM d) = EXTRACT(YEAR FROM CURRENT_DATE)
         AND EXTRACT(MONTH FROM d) = EXTRACT(MONTH FROM CURRENT_DATE)
        THEN 'Mês Atual'
        ELSE TO_CHAR(d, 'YYYY/MM')
    END AS ano_mes_atual,

    (EXTRACT(YEAR FROM d) * 100 + EXTRACT(MONTH FROM d))::int AS ano_mes_ordem,

    CASE EXTRACT(MONTH FROM d)
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS nome_mes,

    CASE EXTRACT(MONTH FROM d)
        WHEN 1 THEN 'Jan'
        WHEN 2 THEN 'Fev'
        WHEN 3 THEN 'Mar'
        WHEN 4 THEN 'Abr'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Jun'
        WHEN 7 THEN 'Jul'
        WHEN 8 THEN 'Ago'
        WHEN 9 THEN 'Set'
        WHEN 10 THEN 'Out'
        WHEN 11 THEN 'Nov'
        WHEN 12 THEN 'Dez'
    END AS nome_mes_abrev,

    CASE EXTRACT(ISODOW FROM d)
        WHEN 1 THEN 'Segunda-feira'
        WHEN 2 THEN 'Terça-feira'
        WHEN 3 THEN 'Quarta-feira'
        WHEN 4 THEN 'Quinta-feira'
        WHEN 5 THEN 'Sexta-feira'
        WHEN 6 THEN 'Sábado'
        WHEN 7 THEN 'Domingo'
    END AS nome_dia,

    CASE EXTRACT(ISODOW FROM d)
        WHEN 1 THEN 'Seg'
        WHEN 2 THEN 'Ter'
        WHEN 3 THEN 'Qua'
        WHEN 4 THEN 'Qui'
        WHEN 5 THEN 'Sex'
        WHEN 6 THEN 'Sáb'
        WHEN 7 THEN 'Dom'
    END AS nome_dia_abrev,

    EXTRACT(ISODOW FROM d)::int AS dia_semana,
    EXTRACT(WEEK FROM d)::int AS semana_iso,
    EXTRACT(ISOYEAR FROM d)::int AS ano_iso,
    EXTRACT(QUARTER FROM d)::int AS trimestre,

    TO_CHAR(d, 'DD/MM') AS dia_mes_abr,

    EXTRACT(ISODOW FROM d) IN (6,7) AS is_fim_de_semana,
    FALSE AS is_feriado,
    NULL AS nome_feriado
FROM (
    SELECT DISTINCT data::date AS d
    FROM useall.gold_requisicoes
    WHERE data IS NOT NULL
) x
ON CONFLICT (data) DO NOTHING;
""")

# ---------------- EXECUÇÃO ----------------
with engine.begin() as conn:
    conn.execute(sql_create_dim_calendario)
    conn.execute(sql_create_indices)
    conn.execute(sql_atualiza_calendario)

# %%
import os
from pathlib import Path
from sqlalchemy import text

# 1. Definir a raiz do projeto relativa a este arquivo script
# Path(__file__).parent pega a pasta onde o seu script .py está
BASE_DIR = Path(__file__).resolve().parent

# 2. Montar o caminho relativo para o SQL
# Isso vai funcionar em Windows (\\) e Linux (/) automaticamente
sql_file_path = BASE_DIR / "sql" / "view" / "vw_gold_filiais_uf.sql"

def rodar_script_sql(engine, caminho):
    # Verificar se o arquivo realmente existe no caminho relativo
    if not caminho.exists():
        print(f"❌ Erro: Arquivo não encontrado!")
        print(f"   Buscado em: {caminho}")
        print(f"   Diretório atual de execução: {os.getcwd()}")
        return

    try:
        # Lendo o conteúdo do SQL
        query = caminho.read_text(encoding='utf-8')

        # Executando no banco
        with engine.begin() as conn:
            conn.execute(text(query))
            print(f"✅ View processada com sucesso via caminho relativo!")
            
    except Exception as e:
        print(f"⚠️ Erro na execução: {e}")

# Chamar a função
rodar_script_sql(engine, sql_file_path)


