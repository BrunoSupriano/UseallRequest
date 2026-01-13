# --- Definição dos Filtros Complexos ---

filtros_req = [
    {"Nome": "IDFILIAL", "Valor": [333,334,335,336,387,404,520,558,578,339,340,342,343,341,344,345,346,381,389,390], "Operador": 1, "Descricao": "Filial", "ValorFormatado": "SETUP AUTOMACAO E SEGURANCA", "TipoPeriodoData": None},
    {"Nome": "DATA", "Valor": "01/01/1900,01/01/2027", "Operador": 8, "Descricao": "Data da requisição", "ValorFormatado": "01/01/1900 até 01/01/2027", "TipoPeriodoData": 5},
    {"Nome": "DATAPREVATEND", "Valor": "01/01/1900,01/01/2027", "Operador": 8, "Descricao": "Previsão atendimento", "ValorFormatado": "01/01/1900 até 01/01/2027", "TipoPeriodoData": 8},
    {"Nome": "CLASSGRUPOITEM", "Valor": ""},
    {"Nome": "CLASSCONTACDC", "Valor": ""},
    {"Nome": "quebra", "Valor": 1},
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
