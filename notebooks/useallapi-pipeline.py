
"""
Pipeline Useall -> Postgres (Staging -> Silver -> Gold)
Compatível com Airflow 3.1.7 e Execução Local via Terminal.
"""

import os
import pendulum
import json
import time
import logging
import requests # type: ignore
import pandas as pd # type: ignore
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv # type: ignore
from sqlalchemy import create_engine, text # type: ignore
from urllib.parse import quote

try:
    from airflow import DAG # type: ignore
    from airflow.providers.standard.operators.python import PythonOperator # type: ignore
    from airflow.sdk import TaskGroup # type: ignore
except Exception:
    # Mock para execução local sem Airflow instalado ou com erro de config
    from unittest.mock import MagicMock
    DAG = MagicMock()
    PythonOperator = MagicMock()
    TaskGroup = MagicMock()


# ================= CONFIGURAÇÃO =================
# Tenta carregar .env local se existir
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Caminhos para Docker/Linux vs Windows/Local
if os.path.exists("/.dockerenv"):
    # Estamos no Docker
    ENV_PATH = "/opt/airflow/useall/.env"
    TOOLS_DIR = "/opt/airflow/tools"
else:
    # Estamos no Windows / Local
    # Assume que o .env está na raiz do projeto ou na pasta config
    ENV_PATH = os.path.join(BASE_DIR, "..", ".env") 
    if not os.path.exists(ENV_PATH):
        ENV_PATH = os.path.join(BASE_DIR, ".env")
    TOOLS_DIR = r"C:\Repositorio\Tools"

load_dotenv(ENV_PATH, override=True)

# Variáveis
BASE_URL = os.getenv("USEALL_BASE_URL")
TOKEN = os.getenv("USEALL_TOKEN")
HEADERS = {
    "accept": "application/json",
    "use-relatorio-token": TOKEN,
}

# Banco de Dados
DB_User = quote(os.getenv("PG_USER", "postgres"))
DB_Pass = quote(os.getenv("PG_PASSWORD", "4102"))
DB_Host = os.getenv("PG_HOST", "localhost")
DB_Port = os.getenv("PG_PORT", "5433")
DB_Name = os.getenv("PG_DBNAME", "SETUP")
DB_Schema = os.getenv("DB_SCHEMA", "useall")

# Ajuste para Docker
if os.path.exists("/.dockerenv") and DB_Host == "localhost":
    DB_Host = "host.docker.internal"

DB_URL = f"postgresql+psycopg2://{DB_User}:{DB_Pass}@{DB_Host}:{DB_Port}/{DB_Name}"
engine = create_engine(DB_URL)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("useall_pipeline")

# ================= FUNÇÕES AUXILIARES =================

def ensure_schema(**context):
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_Schema}"))
        conn.commit()
    logger.info(f"Schema {DB_Schema} garantido.")

def buscar_dados_api(identificacao, nome_arquivo, backend_filters=None, extra_params=None):
    query_params = {"Identificacao": identificacao}
    
    if backend_filters:
        query_params["FiltrosSqlQuery"] = json.dumps(backend_filters, ensure_ascii=False)
    if extra_params:
        query_params.update(extra_params)
    
    logger.info(f"Extraindo: {nome_arquivo}...")
    
    while True:
        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=query_params, timeout=500)
            if response.status_code == 429:
                time.sleep(185)
                continue
            response.raise_for_status()
            data = response.json()
            registros = data.get("data") if isinstance(data, dict) else data
            
            if registros:
                return pd.DataFrame(registros)
            return pd.DataFrame()
            
        except requests.exceptions.Timeout:
            time.sleep(60)
            continue
        except Exception as e:
            logger.error(f"Erro em {nome_arquivo}: {e}")
            raise

def save_to_postgres(df, table_name, if_exists="replace"):
    if df is not None and not df.empty:
        df.to_sql(table_name, engine, schema=DB_Schema, if_exists=if_exists, index=False)
        logger.info(f"Tabela {DB_Schema}.{table_name} salva com sucesso ({len(df)} regs).")
    else:
        logger.warning(f"DataFrame vazio para {table_name}. Nada salvo.")

def filtro_simples(nome, valor):
    return {"Nome": nome, "Valor": valor}

# ================= TASKS DE EXTRAÇÃO =================

def task_extract_simples(**context):
    params_fixos = {"pagina": 1, "qtderegistros": 1}
    
    tarefas = [
        {"nome": "staging_itens", "id": "m2_estoque_item", "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")]},
        {"nome": "staging_unidades", "id": "m2_estoque_unidade", "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")]},
        {"nome": "staging_segmentos", "id": "m2_vendas_segmento", "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")]},
        {"nome": "staging_cidades", "id": "m2_geral_cidades", "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")]},
        {"nome": "staging_solcompra", "id": "m2_compras_m2_compras_solicitacao_de_compras__extra", "filtros": [filtro_simples("DATAINI", "01/01/1900"), filtro_simples("DataFim", "01/01/2027")]},
        {"nome": "staging_filiais", "id": "m2_geral_filiais", "filtros": [filtro_simples("DATAHORAALTINI", "01/01/1900, 11:00:00"), filtro_simples("DATAHORAALTFIM", "01/01/2027, 14:00:00")]},
        {"nome": "staging_empresas", "id": "m2_geral_empresas", "filtros": [filtro_simples("DATAHORAALTINI", "01/01/2022, 11:00:00"), filtro_simples("DATAHORAALTFIM", "01/01/2027, 14:00:00")]},
        {"nome": "staging_expedicao", "id": "m2_vendas_extracao_de_dados__saida_expedicao", "filtros": [filtro_simples("data1", "01/01/1900"), filtro_simples("data2", "01/01/2027")]},
        {"nome": "staging_clientesfornecedores", "id": "m2_geral_clientes__fornecedores", "filtros": [filtro_simples("DATAHORAALTERACAOINI", "01/01/1900"), filtro_simples("DATAHORAALTERACAOFIM", "01/01/2027")]},
        {"nome": "staging_almoxarifados", "id": "m2_estoque_almoxarifados", "filtros": [filtro_simples("DATAHORAALTINI", "01/01/1900"), filtro_simples("DATAHORAALTFIM", "01/01/2027")]},
    ]

    for t in tarefas:
        df = buscar_dados_api(t["id"], t["nome"], t["filtros"], params_fixos)
        save_to_postgres(df, t["nome"])

def task_extract_complexas(**context):
    # Requisições
    filtros_req = [
        {"Nome": "IDFILIAL", "Valor": [333, 339, 340, 381, 389, 336, 387, 520, 404, 558, 578, 341, 390, 345, 344, 346, 335, 334, 342, 343], "Operador": 1},
        {"Nome": "DATA", "Valor": "01/01/2010,01/01/2027", "Operador": 8, "TipoPeriodoData": 5},
        {"Nome": "DATAPREVATEND", "Valor": "01/01/2010,01/01/2027", "Operador": 8, "TipoPeriodoData": 8},
        {"Nome": "CLASSGRUPOITEM", "Valor": ""},
        {"Nome": "CLASSCONTACDC", "Valor": ""},
        {"Nome": "quebra", "Valor": 1},
        {"Nome": "FILTROSWHERE", "Valor": " AND IDEMPRESA = 211"},
    ]
    df_req = buscar_dados_api("m2_estoque_requisicao_de_materiais", "staging_requisicoes", filtros_req)
    save_to_postgres(df_req, "staging_requisicoes")

    # Atendimentos
    filtros_atend = [{"Nome": "FILTROSWHERE", "Valor": ("WHERE IDEMPRESA = 211 "
            "AND IDFILIAL IN (333,339, 340, 381, 389, 336, 387, 520, 404, 558, 578, 341, 390, 345, 344, 346, 335, 334, 342, 343) "
            "AND DATA_REQ BETWEEN '01/01/1900' AND '01/01/2900' "
            "AND DATA_ATEND BETWEEN '01/01/1900' AND '01/01/2900'")}]
    params_atend = {"NomeOrganizacao": "SETUP SERVICOS ESPECIALIZADOS LTDA", "Parametros": json.dumps([{"Nome": "usecellmerging", "Valor": True}, {"Nome": "quebra", "Valor": 0}])}
    
    df_atend = buscar_dados_api("m2_estoque_atendimentos_de_requisicao", "staging_atendimentodereq", filtros_atend, params_atend)
    save_to_postgres(df_atend, "staging_atendimentodereq")


def task_extract_custos(**context):
    grupos = {
        "ctfm": [342, 343],
        "lojas": [335, 334],
        "vm": [345, 344, 346],
        "setup_automacao": [333],
        "servicos": [339, 340, 381, 389],
        "setup": [336, 387, 520, 404, 558, 578, 341, 390],
    }
    target_table = "raw_custos_grupos"
    data_ref = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y")

    for grupo, ids in grupos.items():
        # Check já carregado
        query_check = f"SELECT 1 FROM {DB_Schema}.{target_table} WHERE _grupo_origem = '{grupo}' AND data_carga::date = CURRENT_DATE LIMIT 1"
        try:
            with engine.connect() as conn:
                loaded = conn.execute(text(query_check)).scalar()
        except:
            loaded = False
        
        if loaded:
            logger.info(f"Grupo {grupo} já carregado hoje. Pulando.")
            continue
            
        filtros = [
            {"Nome": "idfilial", "Valor": ids, "Operador": 1},
            {"Nome": "FILTROSREGISTROSATIVO", "Valor": ""},
            {"Nome": "filtroswhere", "Valor": f" AND IDFILIAL IN ({','.join(map(str, ids))})"},
            {"Nome": "data", "Valor": data_ref},
        ]
        
        df = buscar_dados_api("m2_estoque_custos", f"custos_{grupo}", filtros)
        if df is not None and not df.empty:
            df["_grupo_origem"] = grupo
            df["data_carga"] = datetime.now()
            
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {DB_Schema}.{target_table} WHERE _grupo_origem = '{grupo}'"))
            
            df.to_sql(target_table, engine, schema=DB_Schema, if_exists="append", index=False)
            logger.info(f"Grupo {grupo} salvo.")
            time.sleep(60) # Cooldown

    # Snapshot
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS {DB_Schema}.staging_custos_raw CASCADE"))
    except: pass

    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {DB_Schema}.staging_custos_raw CASCADE"))
    except: pass
    
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {DB_Schema}.staging_custos_raw AS SELECT * FROM {DB_Schema}.raw_custos_grupos"))
        logger.info("Snapshot Custos criado.")

def task_extract_estoque(**context):
    data_atual = datetime.strptime("01/01/2026", "%d/%m/%Y")
    data_fim = datetime.now()
    target_table = "staging_estoque_diario"

    while data_atual <= data_fim:
        data_br = data_atual.strftime("%d/%m/%Y")
        data_iso = data_atual.strftime("%Y-%m-%d")
        
        try:
            with engine.connect() as conn:
                exists = conn.execute(text(f"SELECT 1 FROM {DB_Schema}.{target_table} WHERE data_referencia = '{data_iso}' LIMIT 1")).scalar()
        except:
            exists = False
            
        if exists:
            data_atual += timedelta(days=1)
            continue
            
        filtros = [
            {"Nome": "ADDATA", "Valor": data_br},
            {"Nome": "ANQUEBRA", "Valor": 0},
            {"Nome": "FILTROSWHEREFORNEC", "Valor": ""},
            {"Nome": "FILTROSREGISTROSATIVO", "Valor": " AND ITEM.ATIVO = 1 AND ALMOX.ATIVO = 1 AND ITEMALMOX.ATIVO = 1"},
            {"Nome": "FILTROSWHERE", "Valor": " AND EXISTS(SELECT 1 FROM USE_USUARIOS_FILIAIS UFILIAIS WHERE UFILIAIS.IDEMPRESA = T.IDEMPRESA AND UFILIAIS.IDFILIAL = T.IDFILIAL AND UFILIAIS.IDUSUARIO = 7332) AND T.IDFILIAL in (333)"},
        ]
        
        df = buscar_dados_api("m2_estoque_saldo_de_estoque", f"estoque_{data_iso}", filtros)
        if df is not None and not df.empty:
            df["data_referencia"] = data_iso
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {DB_Schema}.{target_table} WHERE data_referencia = '{data_iso}'"))
            df.to_sql(target_table, engine, schema=DB_Schema, if_exists="append", index=False)
            logger.info(f"Estoque {data_iso} salvo.")
            
        data_atual += timedelta(days=1)
        time.sleep(1)


# ================= TASKS SILVER / GOLD =================

def task_run_analytics(**context):
    try:
        import sys
        # Adiciona o diretório Tools ao path se não estiver lá
        if TOOLS_DIR not in sys.path:
            sys.path.append(TOOLS_DIR)
        
        from type_analytics import TypeInferenceEngine # type: ignore
        
        engine_silver = create_engine(DB_URL)
        analytics = TypeInferenceEngine(engine=engine_silver, schema=DB_Schema)
        analytics.process_tables()
    except Exception as e:
        logger.error(f"Erro TypeAnalytics: {e}")
        # Não falha a DAG, apenas loga
        pass

def task_materialize_gold(**context):
    # Logica de Gold SQLs complexos
    sqls = [
        # Gold genérico
        """
        DO $$ DECLARE r RECORD; gold_tbl TEXT; BEGIN
            FOR r IN SELECT table_name FROM information_schema.tables WHERE table_schema = 'useall' AND table_name LIKE 'silver_%' LOOP
                gold_tbl := replace(r.table_name, 'silver_', 'gold_');
                EXECUTE format('DROP TABLE IF EXISTS useall.%I CASCADE; CREATE TABLE useall.%I AS SELECT * FROM useall.%I;', gold_tbl, gold_tbl, r.table_name);
            END LOOP;
        END $$;
        """,
        # View Custos
        """CREATE OR REPLACE VIEW useall.vw_ultimos_custos AS SELECT * FROM (SELECT gc.*, ROW_NUMBER() OVER (PARTITION BY codigoitem ORDER BY datacusto DESC) AS rn FROM useall.gold_custos gc WHERE ultimocusto <> 0) t WHERE rn = 1;""",
        # Gold Filiais
        """DROP TABLE IF EXISTS useall.gold_filiais CASCADE; CREATE TABLE useall.gold_filiais AS SELECT *, CASE WHEN idfilial IN (393, 336, 337, 558, 387) THEN 'RS' WHEN idfilial = 520 THEN 'BA' WHEN idfilial = 404 THEN 'DF' WHEN idfilial IN (342, 343, 381, 389, 334, 335, 339, 333, 341, 578, 390, 379, 344, 345, 346, 338) THEN 'SC' ELSE '*NOVA' END AS uf FROM useall.silver_filiais;""",
        # Gold Atendimentos
        """DROP TABLE IF EXISTS useall.gold_atendimentodereq CASCADE; CREATE TABLE useall.gold_atendimentodereq AS SELECT *, idreqmat::text || '-' || iditem::text AS py_idreqitem, iditem::text || '-' || TO_CHAR(data_atend::date, 'YYYYMMDD') AS py_iddataitem FROM useall.silver_atendimentodereq;""",
        # Gold Estoque
        """DROP TABLE IF EXISTS useall.gold_estoque CASCADE; CREATE TABLE useall.gold_estoque AS SELECT e.*, c.ultimocusto, CASE WHEN e.estoquemin IS NOT NULL AND e.estoquemax IS NOT NULL AND e.estoquemin > e.estoquemax THEN 'PARAMETRO_INVALIDO' WHEN e.saldodisponivel < 0 THEN 'INCONSISTENTE' WHEN e.estoquemin IS NULL OR e.estoquemin = 0 THEN 'SEM_MINIMO' WHEN e.estoquemin > 0 AND e.saldodisponivel = 0 THEN 'RUPTURA' WHEN e.estoquemin > 0 AND e.saldodisponivel > 0 AND e.saldodisponivel < e.estoquemin THEN 'CRITICO' WHEN e.estoquemax IS NOT NULL AND e.saldodisponivel > e.estoquemax THEN 'EXCESSO' WHEN e.saldodisponivel >= e.estoquemin AND (e.estoquemax IS NULL OR e.saldodisponivel <= e.estoquemax) THEN 'ADEQUADO' ELSE 'NAO_CLASSIFICADO' END AS py_status_estoque, CASE WHEN e.estoquemin IS NOT NULL AND e.estoquemax IS NOT NULL AND e.estoquemin > e.estoquemax THEN 0 WHEN e.estoquemin IS NULL OR e.estoquemin = 0 THEN 0 WHEN e.saldodisponivel < e.estoquemin THEN (e.estoquemin - GREATEST(e.saldodisponivel, 0)) * COALESCE(c.ultimocusto, 0) WHEN e.estoquemax IS NOT NULL AND e.saldodisponivel > e.estoquemax THEN -1 * (e.saldodisponivel - e.estoquemax) * COALESCE(c.ultimocusto, 0) ELSE 0 END AS valor_impacto_estoque FROM useall.silver_estoque e LEFT JOIN useall.vw_ultimos_custos c ON c.codigoitem = e.iditem;""",
        # Gold Requisicoes
        """DROP TABLE IF EXISTS useall.gold_requisicoes CASCADE; CREATE TABLE useall.gold_requisicoes AS SELECT r.*, CASE status::int WHEN 0 THEN 'Digitado' WHEN 1 THEN 'Aberto' WHEN 3 THEN 'Cancelado' WHEN 10 THEN 'Parcial' WHEN 11 THEN 'Atendido' ELSE 'Desconhecido' END AS py_desc_status, CASE WHEN r.quantcancel = r.quant THEN 'CANCELADO TOTAL' WHEN r.quantsubst = r.quant THEN 'SUBSTITUIDO TOTAL' WHEN r.quantatend = 0 AND r.saldo > 0 THEN 'NÃO ATENDIDA' WHEN r.quantatend = r.quant THEN 'ATENDIDO' WHEN r.quantatend > r.quant THEN 'ATENDIDO A MAIS' WHEN r.quantatend < r.quant AND r.quantatend > 0 THEN 'ATENDIDA PARCIAL' ELSE 'INDEFINIDO' END AS py_status_item, CASE WHEN r.quantatend > 0 THEN 'SIM' ELSE 'NÃO' END AS py_gera_atend, r.idreqmat::text || '-' || r.iditem::text AS py_idreqitem, COALESCE(ati.max_dataatend_item, atr.max_dataatend_req) AS py_data_ult_atend FROM useall.silver_requisicoes r LEFT JOIN (SELECT py_idreqitem, MAX(data_atend) AS max_dataatend_item FROM useall.gold_atendimentodereq GROUP BY py_idreqitem) ati ON ati.py_idreqitem = (r.idreqmat::text || '-' || r.iditem::text) LEFT JOIN (SELECT idreqmat, MAX(data_atend) AS max_dataatend_req FROM useall.gold_atendimentodereq GROUP BY idreqmat) atr ON atr.idreqmat = r.idreqmat;""",
        # Gold Estoque Diario
        """DROP TABLE IF EXISTS useall.gold_estoque_diario CASCADE; CREATE TABLE useall.gold_estoque_diario AS SELECT *, CONCAT(iditem, '-', TO_CHAR(data_referencia::date, 'YYYYMMDD')) AS py_iddataitem FROM useall.silver_estoque_diario WHERE desc_almox = 'MERC. MATRIZ';"""
    ]
    
    with engine.begin() as conn:
        for s in sqls:
            conn.execute(text(s))
    logger.info("Camada Gold materializada.")

def task_dim_calendario(**context):
    sql = """
    CREATE TABLE IF NOT EXISTS useall.dim_calendario (data DATE PRIMARY KEY, ano INT, mes INT, dia INT, ano_mes TEXT, ano_mes_atual TEXT, ano_mes_ordem INT, nome_mes TEXT, nome_mes_abrev TEXT, nome_dia TEXT, nome_dia_abrev TEXT, dia_semana INT, semana_iso INT, ano_iso INT, trimestre INT, dia_mes_abr TEXT, is_fim_de_semana BOOLEAN, is_feriado BOOLEAN, nome_feriado TEXT);
    
    INSERT INTO useall.dim_calendario (data, ano, mes, dia, ano_mes, ano_mes_atual, ano_mes_ordem, nome_mes, nome_mes_abrev, nome_dia, nome_dia_abrev, dia_semana, semana_iso, ano_iso, trimestre, dia_mes_abr, is_fim_de_semana, is_feriado, nome_feriado)
    SELECT DISTINCT d::date, EXTRACT(YEAR FROM d)::int, EXTRACT(MONTH FROM d)::int, EXTRACT(DAY FROM d)::int, TO_CHAR(d, 'YYYY/MM'), CASE WHEN EXTRACT(YEAR FROM d) = EXTRACT(YEAR FROM CURRENT_DATE) AND EXTRACT(MONTH FROM d) = EXTRACT(MONTH FROM CURRENT_DATE) THEN 'Mês Atual' ELSE TO_CHAR(d, 'YYYY/MM') END, (EXTRACT(YEAR FROM d) * 100 + EXTRACT(MONTH FROM d))::int, CASE EXTRACT(MONTH FROM d) WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março' WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho' WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro' WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro' END, CASE EXTRACT(MONTH FROM d) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Fev' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Abr' WHEN 5 THEN 'Mai' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago' WHEN 9 THEN 'Set' WHEN 10 THEN 'Out' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dez' END, CASE EXTRACT(ISODOW FROM d) WHEN 1 THEN 'Segunda-feira' WHEN 2 THEN 'Terça-feira' WHEN 3 THEN 'Quarta-feira' WHEN 4 THEN 'Quinta-feira' WHEN 5 THEN 'Sexta-feira' WHEN 6 THEN 'Sábado' WHEN 7 THEN 'Domingo' END, CASE EXTRACT(ISODOW FROM d) WHEN 1 THEN 'Seg' WHEN 2 THEN 'Ter' WHEN 3 THEN 'Qua' WHEN 4 THEN 'Qui' WHEN 5 THEN 'Sex' WHEN 6 THEN 'Sáb' WHEN 7 THEN 'Dom' END, EXTRACT(ISODOW FROM d)::int, EXTRACT(WEEK FROM d)::int, EXTRACT(ISOYEAR FROM d)::int, EXTRACT(QUARTER FROM d)::int, TO_CHAR(d, 'DD/MM'), EXTRACT(ISODOW FROM d) IN (6,7), FALSE, NULL
    FROM (SELECT DISTINCT data::date AS d FROM useall.gold_requisicoes WHERE data IS NOT NULL) x
    ON CONFLICT (data) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info("Dim Calendario atualizada.")


# ================= DAG DEFINITION =================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="useall_pipeline_v3",
    description="Pipeline Useall -> Postgres (Staging -> Silver -> Gold)",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    schedule="0 6,8,10,12,14,16,18 * * *",
    catchup=False,
    tags=["useall", "postgres", "etl"],
) as dag:

    start = PythonOperator(task_id="start_pipeline", python_callable=ensure_schema)

    with TaskGroup("extraction_group", tooltip="Extração de Dados") as extraction_group:
        ext_simples = PythonOperator(task_id="extract_simples", python_callable=task_extract_simples)
        ext_complexas = PythonOperator(task_id="extract_complexas", python_callable=task_extract_complexas)
        ext_custos = PythonOperator(task_id="extract_custos", python_callable=task_extract_custos)
        ext_estoque = PythonOperator(task_id="extract_estoque", python_callable=task_extract_estoque)
        
        # Paralelismo
        [ext_simples, ext_complexas, ext_custos, ext_estoque]

    silver = PythonOperator(task_id="run_silver_analytics", python_callable=task_run_analytics)

    with TaskGroup("gold_group", tooltip="Transformação Gold") as gold_group:
        gold_mat = PythonOperator(task_id="materialize_gold", python_callable=task_materialize_gold)
        calendario = PythonOperator(task_id="dim_calendario", python_callable=task_dim_calendario)
        
        gold_mat >> calendario

    end = PythonOperator(task_id="end_pipeline", python_callable=lambda: print("FIM"))

    # Flow
    start >> extraction_group >> silver >> gold_group >> end


# ================= EXECUÇÃO LOCAL =================
if __name__ == "__main__":
    print("--- Execução Local Iniciada ---")
    ensure_schema()
    
    print("\n[1/4] Extração...")
    task_extract_simples()
    task_extract_complexas()
    task_extract_custos()
    task_extract_estoque()
    
    print("\n[2/4] Silver Analytics...")
    task_run_analytics()
    
    print("\n[3/4] Gold Materialization...")
    task_materialize_gold()
    task_dim_calendario()
    
    print("\n[4/4] Finalizado com Sucesso!")
