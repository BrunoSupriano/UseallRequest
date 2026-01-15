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
    "use-relatorio-token": " "  # substitua pelo seu token real
}

# --- Requisição direta ---
response = requests.get(url_completa, headers=headers, timeout=3000)

# --- Converter JSON para DataFrame ---
df = pd.DataFrame(response.json())
print(df.head())'''
