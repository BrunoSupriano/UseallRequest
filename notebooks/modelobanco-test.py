# %% [markdown]
# # Extração de API Useall

# %% [markdown]
# ## Tools and functions

# %%
import pandas as pd
import requests
import json
import urllib.parse
import time

# --- Configurações ---
BASE_URL = "https://extracao.useallcloud.com.br/api/v1/json/"
HEADERS = {
    "accept": "application/json",
    "use-relatorio-token": "eyJJZCI6ImNkMDNkMzhiLWZhNzUtNDg4Yi04NDA1LTg5OTU1MzBjNjFiMSIsIlN0cmluZ0NvbmV4YW8iOiJBeHh4IFh2a2VqST10bGRxZGZ4Rzk7c3FJMSBCbz1ScUl4WHlnYWVoO2dNcXp6djFvPXNxSXVleGd5SW5TSSQkOyIsIkNvZGlnb1VzdWFyaW8iOjczMzIsIkNvZGlnb1RlbmFudCI6MTQzfQ=="
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
# ## Variaveis de filtros

# %% [markdown]
# ### Simples

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
    }
]


# %% [markdown]
# ### COMPLEXAS

# %%

filtros_req = [
    {"Nome": "IDFILIAL", "Valor": [333,334,335,336,387,404,520,558,578,339,340,342,343,341,344,345,346,381,389,390], "Operador": 1},
    {"Nome": "DATA", "Valor": "01/01/2010,01/01/2027", "Operador": 8, "TipoPeriodoData": 5},
    {"Nome": "DATAPREVATEND", "Valor": "01/01/2010,01/01/2027", "Operador": 8, "TipoPeriodoData": 8},
    {"Nome": "CLASSGRUPOITEM", "Valor": ""},
    {"Nome": "CLASSCONTACDC", "Valor": ""},
    {"Nome": "quebra", "Valor": 1},
    {"Nome": "FILTROSWHERE", "Valor": " AND IDEMPRESA = 211"}
]

filtros_estoque = [
    {"Nome": "ADDATA", "Valor": "08/01/2010"},
    {
        "Nome": "FILTROSWHERE",
        "Valor": " AND EXISTS (SELECT 1 FROM USE_USUARIOS_FILIAIS UFILIAIS "
                "WHERE UFILIAIS.IDEMPRESA = T.IDEMPRESA "
                "AND UFILIAIS.IDFILIAL = T.IDFILIAL "
                "AND UFILIAIS.IDUSUARIO = 7332) "
                "AND T.IDFILIAL IN (333,334,336,404,335,387,520,558,578)"
    },
    {"Nome": "ANQUEBRA", "Valor": 0}
]

filtros_atend = [
    {
        "Nome": "FILTROSWHERE",
        "Valor": (
            "WHERE IDEMPRESA = 211 "
            "AND IDFILIAL IN (333,334,335,336,387,404,520,558,339,578,340,342,343,341,344,345,346,381,389,390) "
            "AND DATA_REQ BETWEEN '01/01/1900' AND '01/01/2900' "
            "AND DATA_ATEND BETWEEN '01/01/1900' AND '01/01/2900'"
        )
    }
]

params_atend = {
    "NomeOrganizacao": "SETUP SERVICOS ESPECIALIZADOS LTDA",
    "Parametros": json.dumps([
        {"Nome": "usecellmerging", "Valor": True},
        {"Nome": "quebra", "Valor": 0}
    ])
}

# ===============================
# BLOCO 1 — REQUISIÇÕES
# ===============================

tarefa_requisicoes = {
    "nome": "dfuseallrequisicoes",
    "id": "m2_estoque_requisicao_de_materiais",
    "filtros": filtros_req,
    "extra_params": None
}


# ===============================
# BLOCO 2 — ESTOQUE
# ===============================

tarefa_estoque = {
    "nome": "dfuseallestoque",
    "id": "09249662000174_m2_estoque_saldo_de_estoque__setup",
    "filtros": filtros_estoque,
    "extra_params": None
}

# ===============================
# BLOCO 3 — ATENDIMENTO DE REQUISIÇÕES
# ===============================

tarefa_atendimento = {
    "nome": "dfuseallatendimentodereq",
    "id": "m2_estoque_atendimentos_de_requisicao",
    "filtros": filtros_atend,
    "extra_params": params_atend
}



# %% [markdown]
# ## Criando DataFrames

# %% [markdown]
# ### Usando funções

# %%
carregar_dfs_globais(tarefas_simples)

# %%
carregar_tarefa_complexa(tarefa_estoque)

# %%
carregar_tarefa_complexa(tarefa_requisicoes)

# %%
'''
# ===============================
# BLOCO 4 — CUSTOS DE ESTOQUE
# ===============================

filtros_custos = [
    {"Nome": "IDFILIAL", "Valor": [333,334,335,336,387,404,520,558,339,340,341,342,343,344,345,346,381,389,390], "Operador": 1},
    {"Nome": "FILTROSREGISTROSATIVO", "Valor": "AND IA.ATIVO = 1 AND I.ATIVO = 1"},
    {"Nome": "DATA", "Valor": "13/01/2026"}
]

params_custos = {
    "Parametros": [
        {"Nome": "usecellmerging", "Valor": True},
        {"Nome": "filter", "Valor": (
            "Filiais = SETUP AUTOMACAO E SEGURANCA, LOJA - ARARANGUA, LOJA - CRICIUMA, "
            "SETUP PELOTAS, SETUP BAHIA, SETUP ELDORADO DO SUL, SETUP BRASILIA, "
            "SETUP OS\u00d3RIO, VIGILANCIA SETUP, SETUP FLORIAN\u00d3POLIS, "
            "PINHEIRINHO SERVI\u00c7OS, SETUP COMERCIO MATRIZ, CTFM - ILLUMINATIO ARARANGUA, "
            "CTFM - ILLUMINATIO CD, VM - DISTRIBUIDORA ARARANGUA MATRIZ, VM - DISTRIBUIDORA CD, "
            "ENGECO PROJETOS E CONSTRUCOES LTDA, VM - DISTRIBUIDORA CRICIUMA, FFW ADMINISTRADORA DE BENS, "
            "SETUP LOCA\u00c7\u00d5ES; Data = 13/01/2026; Custo = Ambos; "
            "Apenas itens com saldo = N\u00e3o; Apenas itens e almoxarifados ativos = Sim"
        )}
    ]
}

tarefa_custos = {
    "nome": "dfuseallcustosestoque",
    "id": "m2_estoque_custos",
    "filtros": filtros_custos,
    "extra_params": params_custos
}

carregar_tarefa_complexa(tarefa_custos)
'''

# %%
'''import requests
import pandas as pd

# --- URL completa com todos os parâmetros codificados ---
url_completa = (
    "https://extracao.useallcloud.com.br/api/v1/json?"
    "Identificacao=m2_estoque_custos&"
    "FiltrosSqlQuery=["
    "%7B%22Nome%22%3A%22idfilial%22%2C%22Valor%22%3A%5B333%2C334%2C335%2C336%2C520%2C387%2C404%2C558%2C578%2C340%2C341%2C339%2C342%2C343%2C344%2C345%2C346%2C381%2C389%2C390%5D%2C%22Operador%22%3A1%2C%22Descricao%22%3A%22Filial%22%2C%22ValorFormatado%22%3A%22SETUP%20AUTOMACAO%20E%20SEGURANCA%2C%20LOJA%20-%20ARARANGUA%2C%20LOJA%20-%20CRICIUMA%2C%20SETUP%20ELDORADO%20DO%20SUL%2C%20SETUP%20BAHIA%2C%20SETUP%20PELOTAS%2C%20SETUP%20BRASILIA%2C%20SETUP%20OS%C3%93RIO%2C%20VIGILANCIA%20SETUP%2C%20SETUP%20COMERCIO%20MATRIZ%2C%20PINHEIRINHO%20SERVI%C3%87OS%2C%20CTFM%20-%20ILLUMINATIO%20ARARANGUA%2C%20CTFM%20-%20ILLUMINATIO%20CD%2C%20VM%20-%20DISTRIBUIDORA%20ARARANGUA%20MATRIZ%2C%20VM%20-%20DISTRIBUIDORA%20CD%2C%20VM%20-%20DISTRIBUIDORA%20CRICIUMA%2C%20ENGECO%20PROJETOS%20E%20CONSTRUCOES%20LTDA%2C%20FFW%20ADMINISTRADORA%20DE%20BENS%2C%20SETUP%20LOCA%C3%87%C3%95ES%22%2C%22TipoPeriodoData%22%3Anull%7D%2C"
    "%7B%22Nome%22%3A%22FILTROSREGISTROSATIVO%22%2C%22Valor%22%3A%22%20AND%20IA.ATIVO%20%3D%201%20AND%20I.ATIVO%20%3D%201%22%7D%2C"
    "%7B%22Nome%22%3A%22filtroswhere%22%2C%22Valor%22%3A%22%20AND%20IDFILIAL%20IN%20(333%2C334%2C335%2C336%2C520%2C387%2C404%2C558%2C578%2C340%2C341%2C339%2C342%2C343%2C344%2C345%2C346%2C381%2C389%2C390)%22%7D%2C"
    "%7B%22Nome%22%3A%22data%22%2C%22Valor%22%3A%2213/01/2026%22%7D]"
)

# --- Headers ---
headers = {
    "accept": "application/json",
    "use-relatorio-token": "eyJJZCI6ImNkMDNkMzhiLWZhNzUtNDg4Yi04NDA1LTg5OTU1MzBjNjFiMSIsIlN0cmluZ0NvbmV4YW8iOiJBeHh4IFh2a2VqST10bGRxZGZ4Rzk7c3FJMSBCbz1ScUl4WHlnYWVoO2dNcXp6djFvPXNxSXVleGd5SW5TSSQkOyIsIkNvZGlnb1VzdWFyaW8iOjczMzIsIkNvZGlnb1RlbmFudCI6MTQzfQ=="  # substitua pelo seu token real
}

# --- Requisição direta ---
response = requests.get(url_completa, headers=headers, timeout=3000)

# --- Converter JSON para DataFrame ---
df = pd.DataFrame(response.json())
print(df.head())'''


# %%
carregar_tarefa_complexa(tarefa_atendimento)

# %% [markdown]
# ### Verificando Tipos

# %%
# --- 3. Verificação de Tipos ---
print(f"[{time.strftime('%H:%M:%S')}] --- INICIANDO VERIFICAÇÃO DE TIPOS ---")
verificar_tipos_dados()

# %% [markdown]
# ## Configurações Banco de Dados 

# %%
import io
import time
import psycopg2
from sqlalchemy import create_engine, text

# ---------------- CONFIG ----------------

DB_URL = "postgresql+psycopg2://postgres:4102@localhost:5432/SETUP"

PG_CONN_INFO = {
    "dbname": "SETUP",
    "user": "postgres",
    "password": "4102",
    "host": "localhost",
    "port": 5432,
}

SCHEMA = "useall"

# ---------------------------------------

engine = create_engine(DB_URL)

# garante schema
with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
    conn.commit()


# %% [markdown]
# ## Staging - Bronze - Dados Brutos tipos indefinidos

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

    conn = psycopg2.connect(**PG_CONN_INFO)
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
# ## Silver definindo tipos automaticamente

# %%
SCHEMA = "useall"
SAMPLE_LIMIT = 50000

import pandas as pd
import re

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

# Monta dicionário de metadata
schema_silver = {}

for staging_table in staging_tables:
    if not staging_table.startswith("staging_"):
        continue
    silver_table = silver_table_name(staging_table)
    log(f"Profiling {SCHEMA}.{staging_table} -> {SCHEMA}.{silver_table}")

    df_sample = pd.read_sql(
    f'SELECT * FROM {SCHEMA}."{staging_table}" LIMIT {SAMPLE_LIMIT}',
    engine
    )

    schema_silver[silver_table] = {
        "staging_table": staging_table,
        "columns": {
            col.lower(): {
                **infer_column_type_final(df_sample[col]),
                "source_col": col  # nome REAL na staging
            }
            for col in df_sample.columns
        }
    }

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
        if fmt:
            return f"""
            CASE
                WHEN {col_txt} = '' THEN NULL
                ELSE {col_txt}::{ 'timestamp' if 'H' in fmt else 'date' }
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
# ## Gold - Adicionando novas colunas e agregando valor

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
# ## Dim_Calendario

# %%
from sqlalchemy import create_engine, text

# ---------------- SQL ----------------
sql_create_dim_calendario = text("""
CREATE TABLE IF NOT EXISTS useall.dim_calendario (
    data DATE PRIMARY KEY,

    ano INT,
    mes INT,
    dia INT,

    mes_ano TEXT,
    mes_ano_ordem INT,

    nome_mes TEXT,
    nome_mes_abrev TEXT,
    nome_dia TEXT,
    nome_dia_abrev TEXT,

    dia_semana INT,
    semana_iso INT,
    ano_iso INT,
    trimestre INT,

    is_fim_de_semana BOOLEAN,
    is_feriado BOOLEAN,
    nome_feriado TEXT
);
""")

sql_create_indices = text("""
CREATE INDEX IF NOT EXISTS idx_dim_calendario_data
    ON useall.dim_calendario (data);

CREATE INDEX IF NOT EXISTS idx_dim_calendario_mes_ano_ordem
    ON useall.dim_calendario (mes_ano_ordem);
""")

sql_atualiza_calendario = text("""
INSERT INTO useall.dim_calendario
SELECT DISTINCT
    d::date AS data,

    EXTRACT(YEAR FROM d)::int AS ano,
    EXTRACT(MONTH FROM d)::int AS mes,
    EXTRACT(DAY FROM d)::int AS dia,

    TO_CHAR(d, 'MM/YYYY') AS mes_ano,
    (EXTRACT(YEAR FROM d) * 100 + EXTRACT(MONTH FROM d))::int AS mes_ano_ordem,

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


# %% [markdown]
# # Entendendo Tipos de dados

# %%
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# ================= CONFIG =================
DB_URL = "postgresql+psycopg2://postgres:4102@localhost:5432/SETUP"
SCHEMA = "useall"
FK_MATCH_MIN = 0.8       # mínimo de correspondência para considerar FK
SAMPLE_LIMIT = 1000      # limitar linhas para tabelas grandes

# ================= CONEXÃO =================
engine = create_engine(DB_URL)

# ================= METADADOS =================
cols = pd.read_sql(f"""
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = '{SCHEMA}'
""", engine)

# ================= FILTRO GOLD =================
gold_tables = [t for t in cols.table_name.unique() if t.startswith("gold_") or t == "dim_calendario"]
cols = cols[cols.table_name.isin(gold_tables)]
tables = cols.table_name.unique()

# ================= PKs CANDIDATAS =================
pk_candidatas = {}

for table in tables:
    table_cols = cols[cols.table_name == table].column_name
    for col in table_cols:
        q = f"""
        SELECT COUNT(DISTINCT "{col}") = COUNT("{col}") AS is_pk
        FROM {SCHEMA}."{table}"
        """
        try:
            ok = pd.read_sql(q, engine).iloc[0, 0]
            if ok:
                pk_candidatas.setdefault(table, []).append(col)
        except Exception as e:
            print(f"Erro PK {table}.{col}: {e}")

# ================= PK DF COM DATA_TYPE =================
pk_df = pd.merge(
    pd.DataFrame([
        {"table_name": t, "column_name": c}
        for t, cols_ in pk_candidatas.items()
        for c in cols_
    ]),
    cols[['table_name', 'column_name', 'data_type']],
    on=['table_name', 'column_name'],
    how='left'
)

# ================= FK CANDIDATAS =================
fk_candidatas = []

for _, c_origem in cols.iterrows():
    # candidate columns: tipo compatível e não PK da própria tabela
    for _, c_dest in pk_df.iterrows():
        if c_origem.table_name == c_dest.table_name:
            continue
        if c_origem.data_type != c_dest.data_type:
            continue

        q = f"""
        SELECT 
            COUNT(*) FILTER (WHERE o."{c_origem.column_name}" = d."{c_dest.column_name}")::float / NULLIF(COUNT(*),0) AS match
        FROM (SELECT * FROM {SCHEMA}."{c_origem.table_name}" LIMIT {SAMPLE_LIMIT}) o
        JOIN (SELECT * FROM {SCHEMA}."{c_dest.table_name}" LIMIT {SAMPLE_LIMIT}) d
        ON o."{c_origem.column_name}" = d."{c_dest.column_name}"
        """

        try:
            match = pd.read_sql(q, engine).iloc[0, 0] or 0
            if match >= FK_MATCH_MIN:
                fk_candidatas.append({
                    "origem": c_origem.table_name,
                    "coluna_origem": c_origem.column_name,
                    "destino": c_dest.table_name,
                    "coluna_destino": c_dest.column_name,
                    "confianca": round(match, 2)
                })
        except Exception as e:
            print(f"Erro FK {c_origem.table_name}.{c_origem.column_name} → {c_dest.table_name}.{c_dest.column_name}: {e}")

# ================= GRAFO =================
G = nx.DiGraph()

for fk in fk_candidatas:
    G.add_edge(
        fk["origem"],
        fk["destino"],
        label=f'{fk["coluna_origem"]} → {fk["coluna_destino"]} ({fk["confianca"]})'
    )

# ================= VISUAL =================
plt.figure(figsize=(18, 14))
pos = nx.spring_layout(G, k=0.8)
nx.draw_networkx_nodes(G, pos, node_size=2200, node_color="#007b5e", alpha=0.9)
nx.draw_networkx_labels(G, pos, font_size=10, font_color="white")
nx.draw_networkx_edges(G, pos, arrowstyle='-|>', arrowsize=20, edge_color="#004d3b")
edge_labels = nx.get_edge_attributes(G, 'label')
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8)
plt.title("Relações PK → FK Candidatas (Gold)", fontsize=16)
plt.axis("off")
plt.show()



