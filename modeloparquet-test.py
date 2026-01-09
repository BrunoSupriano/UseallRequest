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

def verificar_tipos_dados(dfs_dict):
    """Exibe os tipos de dados (dtypes) de cada DataFrame no dicionário"""
    print("\n" + "="*40)
    print(f"[{time.strftime('%H:%M:%S')}] VERIFICAÇÃO DE TIPOS DE DADOS")
    print("="*40)
    
    for nome, df in dfs_dict.items():
        print(f"\nDataFrame: {nome}")
        if df is not None and not df.empty:
            print("-" * 30)
            print(df.dtypes)
            print("-" * 30)
        else:
             print("  (Vazio ou não carregado)")

# --- Defines Auxiliares de Filtro ---
def filtro_simples(nome, valor):
    return {"Nome": nome, "Valor": valor, "Operador": None, "Descricao": None, "ValorFormatado": None}

pipeline_start = time.time()
print(f"--- Pipeline iniciada em {time.strftime('%d/%m/%Y %H:%M:%S')} ---")


# %%
# --- Definição dos Filtros Complexos ---

filtros_req = [
    {"Nome": "IDFILIAL", "Valor": [333,334,335,336,387,404,520,558,578,339,340,342,343,341,344,345,346,381,389,390], "Operador": 1, "Descricao": "Filial", "ValorFormatado": "SETUP AUTOMACAO E SEGURANCA", "TipoPeriodoData": None},
    {"Nome": "DATA", "Valor": "01/01/1900,01/01/2900", "Operador": 8, "Descricao": "Data da requisição", "ValorFormatado": "01/01/1900 até 01/01/2900", "TipoPeriodoData": 1},
    {"Nome": "DATAPREVATEND", "Valor": "01/01/1900,01/01/2900", "Operador": 8, "Descricao": "Previsão atendimento", "ValorFormatado": "01/01/1900 até 01/01/2900", "TipoPeriodoData": 1},
    {"Nome": "CLASSGRUPOITEM", "Valor": ""},
    {"Nome": "CLASSCONTACDC", "Valor": ""},
    {"Nome": "quebra", "Valor": 0},
    {"Nome": "FILTROSWHERE", "Valor": " AND IDEMPRESA = 211"}
]

filtros_estoque = [
    {"Nome": "ADDATA", "Valor": "08/01/2026"},
    {"Nome": "FILTROSWHERE", "Valor": " AND EXISTS (SELECT 1 FROM USE_USUARIOS_FILIAIS UFILIAIS WHERE UFILIAIS.IDEMPRESA = T.IDEMPRESA AND UFILIAIS.IDFILIAL = T.IDFILIAL AND UFILIAIS.IDUSUARIO = 7332) AND T.IDFILIAL IN (333,334,336,404,335,387,520,558,578)"},
    {"Nome": "ANQUEBRA", "Valor": 0}
]

filtros_atend = [{"Nome": "FILTROSWHERE", "Valor": "WHERE IDEMPRESA = 211 AND IDFILIAL IN (333,334,335,336,387,404,520,558,339,578,340,342,343,341,344,345,346,381,389,390) AND DATA_REQ >= '01/01/1900' AND DATA_REQ <= '01/01/2900' AND DATA_ATEND >= '01/01/1900' AND DATA_ATEND <= '01/01/2900'"}]
params_atend = {
    "NomeOrganizacao": "SETUP SERVICOS ESPECIALIZADOS LTDA",
    "Parametros": json.dumps([
        {"Nome": "usecellmerging", "Valor": True},
        {"Nome": "quebra", "Valor": 0},
        {"Nome": "filter", "Valor": "Filial: SETUP AUTOMACAO E SEGURANCA, LOJA - ARARANGUA, LOJA - CRICIUMA\nData requisição: 01/01/1900 até 01/01/2900\nData atendimento: 01/01/1900 até 01/01/2900"}
    ])
}

# --- Lista Unificada de Tarefas ---

params_fixos = {"pagina": 1, "qtderegistros": 1}

tarefas = [
    # Simples
    {
        "nome": "dfuseallitens",
        "id": "m2_estoque_item",
        "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallunidades",
        "id": "m2_estoque_unidade",
        "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallsegmentos",
        "id": "m2_vendas_segmento",
        "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallcidades",
        "id": "m2_geral_cidades",
        "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallsolcompra",
        "id": "m2_compras_m2_compras_solicitacao_de_compras__extra",
        "filtros": [filtro_simples("DataFim", "01/01/2500"), filtro_simples("DATAINI", "01/01/1900")],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallfiliais",
        "id": "m2_geral_filiais",
        "filtros": [filtro_simples("DATAHORAALTINI", "01/01/1900, 11:00:00"), filtro_simples("DATAHORAALTFIM", "01/01/2500, 14:00:00")],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallempresas",
        "id": "m2_geral_empresas",
        "filtros": [filtro_simples("DATAHORAALTINI", "01/01/2022, 11:00:00"), filtro_simples("DATAHORAALTFIM", "01/01/2027, 14:00:00")],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallexpedição",
        "id": "m2_vendas_extracao_de_dados__saida_expedicao",
        "filtros": [filtro_simples("data1", "01/01/1900"), filtro_simples("data2", "01/01/2500")],
        "extra_params": params_fixos
    },
    {
        "nome": "dfuseallclientesfornecedore",
        "id": "m2_geral_clientes__fornecedores",
        "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")],
        "extra_params": params_fixos
    },
    # Complexas
    {
        "nome": "dfuseallrequisicoes",
        "id": "m2_estoque_requisicao_de_materiais",
        "filtros": filtros_req,
        "extra_params": None
    },
    {
        "nome": "dfuseallestoque",
        "id": "09249662000174_m2_estoque_saldo_de_estoque__setup",
        "filtros": filtros_estoque,
        "extra_params": None
    },
    {
        "nome": "dfuseallatendimentodereq",
        "id": "m2_estoque_atendimentos_de_requisicao",
        "filtros": filtros_atend,
        "extra_params": params_atend
    }
]


# %%
# --- 1. Carregamento dos DataFrames (Memória) ---
dfs_carregados = {}

print(f"[{time.strftime('%H:%M:%S')}] --- INICIANDO CARGA EM MEMÓRIA ---")

for t in tarefas:
    df = buscar_dados_api(t["id"], t["nome"], t.get("filtros"), t.get("extra_params"))
    
    if df is not None:
        dfs_carregados[t["nome"]] = df
    else:
        print(f"[{time.strftime('%H:%M:%S')}] Falha ao carregar {t['nome']}")


# %%
# --- 3. Verificação de Tipos ---
print(f"[{time.strftime('%H:%M:%S')}] --- INICIANDO VERIFICAÇÃO DE TIPOS ---")
verificar_tipos_dados(dfs_carregados)

# %%
# --- 2. Exportação para Parquet ---
print(f"[{time.strftime('%H:%M:%S')}] --- INICIANDO EXPORTAÇÃO PARQUET ---")

for nome, df in dfs_carregados.items():
    salvar_parquet(df, nome)

# %%
total_pipeline_time = time.time() - pipeline_start
print(f"--- Pipeline finalizada em {total_pipeline_time:.2f} segundos ---")
print(f"--- Pipeline finalizada em {total_pipeline_time / 60:.2f} minutos ---")

# %%
# - Usado para transformar a lista de DataFrames em variáveis globais
for nome, df in dfs_carregados.items():
    globals()[nome] = df

# %% [markdown]
# ## Staging - Bronze - Dados Brutos tipos indefinidos

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

SCHEMA = "useall_staging"

# ---------------------------------------

engine = create_engine(DB_URL)

# garante schema
with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
    conn.commit()


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

for tarefa in tarefas:
    df_nome = tarefa["nome"]
    df = globals().get(df_nome)

    if df is None or df.empty:
        dfs_nao_encontrados.append(df_nome)
        continue

    tabela = df_nome.replace("dfuseall", "") + "_staging"
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
silver_SCHEMA = "useall_silver"
SAMPLE_LIMIT = 50000

# Garante schema
with engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {silver_SCHEMA}"))

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

# Busca tabelas staging
staging_tables = pd.read_sql(f"""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = '{SCHEMA}' AND table_type='BASE TABLE'
""", engine)["table_name"].tolist()

# Função nome silver
def silver_table_name(staging_table: str) -> str:
    return staging_table.replace("_staging", "_silver") if staging_table.endswith("_staging") else staging_table + "_silver"

# Monta dicionário de metadata
schema_silver = {}

for staging_table in staging_tables:
    silver_table = silver_table_name(staging_table)
    log(f"Profiling {SCHEMA}.{staging_table} -> {silver_SCHEMA}.{silver_table}")

    df_sample = pd.read_sql(f'SELECT * FROM {SCHEMA}."{staging_table}" LIMIT {SAMPLE_LIMIT}', engine)
    schema_silver[silver_table] = {
        "staging_table": staging_table,
        "columns": {col: infer_column_type_final(df_sample[col]) for col in df_sample.columns}
    }

# Cria cast SQL
def generate_cast_sql(col, meta):
    col_sql = f'"{col}"'
    col_txt = f'{col_sql}::text'

    if meta["type"] == "boolean":
        return f"""
        CASE
            WHEN lower({col_txt}) IN ('1','true','sim','s','y','yes') THEN true
            WHEN lower({col_txt}) IN ('0','false','nao','n','no') THEN false
            ELSE NULL
        END AS "{col}"
        """

    if meta["type"] == "timestamp":
        fmt = meta.get("format")
        if fmt:
            return f"""
            CASE
                WHEN {col_sql} IS NULL OR {col_txt} = '' THEN NULL
                ELSE to_timestamp({col_txt}, '{fmt}')
            END AS "{col}"
            """
        else:
            return f"""
            CASE
                WHEN {col_sql} IS NULL OR {col_txt} = '' THEN NULL
                ELSE {col_sql}::timestamp
            END AS "{col}"
            """

    if meta["type"] in ("bigint","numeric(18,4)"):
        return f"""
        CASE
            WHEN {col_txt} ~ '^-?\\d+(\\.\\d+)?$' THEN {col_txt}::{meta["type"]}
            ELSE NULL
        END AS "{col}"
        """

    return f'{col_sql}::text AS "{col}"'

# Gera CREATE TABLE
def generate_create_table(schema, table, columns: dict):
    cols = ",\n  ".join(f'"{col}" {meta["type"]}' for col, meta in columns.items())
    return f"""
    DROP TABLE IF EXISTS {schema}."{table}";
    CREATE TABLE {schema}."{table}" (
      {cols}
    );
    """

# Cria tabelas silver
for silver_table, meta in schema_silver.items():
    log(f"Criando tabela silver {silver_SCHEMA}.{silver_table}")
    ddl = generate_create_table(silver_SCHEMA, silver_table, meta["columns"])
    with engine.begin() as conn:
        conn.execute(text(ddl))

# Gera INSERT
def generate_insert_cast(staging_schema, silver_schema, staging_table, silver_table, columns):
    selects = ",\n".join(generate_cast_sql(col, meta) for col, meta in columns.items())
    return f"""
    INSERT INTO {silver_schema}."{silver_table}"
    SELECT
      {selects}
    FROM {staging_schema}."{staging_table}";
    """

# Carrega dados
for silver_table, meta in schema_silver.items():
    staging_table = meta["staging_table"]
    columns = meta["columns"]
    log(f"Carregando dados em {silver_SCHEMA}.{silver_table}")
    sql = generate_insert_cast(SCHEMA, silver_SCHEMA, staging_table, silver_table, columns)
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        log(f"[OK] {silver_SCHEMA}.{silver_table} carregada")
    except Exception as e:
        log(f"[ERRO] {silver_SCHEMA}.{silver_table} -> {e}")

log("--------------------------------------------------")
log("PROCESSO FINALIZADO")



