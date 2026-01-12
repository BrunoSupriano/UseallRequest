# Extração de dados da Useall - Materiais, Estoque...

# API USEAL STATUS DE EXTRAÇÃO

* [X] itens
* [X] unidades
* [X] segmentos
* [X] cidades
* [X] solcompra
* [X] filiais
* [X] empresas
* [X] expedição

* [X] Requisição de Material - 675405 linhas
* [X] Saldo de Estoque - 132469 linhas
* [X] Atendimento de Requisições
* [ ] Extrato de Estoque


### Requisição de Materiais: Ajustar os filtros conforme desejado.

Link: https://extracao.useallcloud.com.br/api/v1/json?Identificacao=m2_estoque_requisicao_de_materiais&FiltrosSqlQuery=[{"Nome":"IDFILIAL","Valor":[333],"Operador":1,"Descricao":"Filial","ValorFormatado":"SETUP AUTOMACAO E SEGURANCA","TipoPeriodoData":null},{"Nome":"DATA","Valor":"01/01/2026,31/01/2026","Operador":8,"Descricao":"Data da requisi\u00e7\u00e3o","ValorFormatado":"01/01/2026 at\u00e9 31/01/2026","TipoPeriodoData":8},{"Nome":"DATAPREVATEND","Valor":"01/01/2026,31/01/2026","Operador":8,"Descricao":"Previs\u00e3o atendimento","ValorFormatado":"01/01/2026 at\u00e9 31/01/2026","TipoPeriodoData":8},{"Nome":"IDREQUISITANTE","Valor":[1648943],"Operador":1,"Descricao":"Requisitante","ValorFormatado":"WILLIAM MICHELS - DESPESAS SETUP (3)","TipoPeriodoData":null},{"Nome":"IDALMOX","Valor":[2515],"Operador":1,"Descricao":"Almoxarifado","ValorFormatado":"MERC. MATRIZ (7)","TipoPeriodoData":null},{"Nome":"IDITEM","Valor":[403275],"Operador":1,"Descricao":"Item","ValorFormatado":"PILHA 12V A27 ALCALINA ELGIN (50)","TipoPeriodoData":null},{"Nome":"STATUS","Valor":[0,1,3,10,11],"Operador":1,"Descricao":"Status","ValorFormatado":"Digitado, Aberto, Cancelado, Parcial, Atendido","TipoPeriodoData":null},{"Nome":"idreqmattenant","Valor":123,"Operador":6,"Descricao":"Requisi\u00e7\u00e3o","ValorFormatado":123,"TipoPeriodoData":null},{"Nome":"CLASSGRUPOITEM","Valor":""},{"Nome":"CLASSCONTACDC","Valor":""},{"Nome":"quebra","Valor":0},{"Nome":"FILTROSWHERE","Valor":" AND IDEMPRESA =  211"}]

### Atendimento de Requisições: Ajustar os filtros conforme desejado.

Link: https://extracao.useallcloud.com.br/api/v1/json?Identificacao=m2_estoque_atendimentos_de_requisicao&FiltrosSqlQuery=[{"Nome":"FILTROSWHERE","Valor":"WHERE IDEMPRESA = 211 AND IDFILIAL IN (336) AND DATA_REQ >= '01/01/2026' AND DATA_REQ <= '31/01/2026' AND DATA_ATEND >= '01/01/2026' AND DATA_ATEND <= '31/01/2026' AND IDALMOX IN (2515) AND IDITEM IN (613733) AND IDREQMATTENANT = 123"}]

,"NomeOrganizacao":"SETUP SERVICOS ESPECIALIZADOS LTDA","Parametros":[{"Nome":"usecellmerging","Valor":true},{"Nome":"quebra","Valor":0},{"Nome":"filter","Valor":"Filial: SETUP AUTOMACAO E SEGURANCA, LOJA - ARARANGUA, LOJA - CRICIUMA, SETUP ELDORADO DO SUL, SETUP PELOTAS, SETUP BRASILIA, SETUP BAHIA, SETUP OS\u00d3RIO, PINHEIRINHO SERVI\u00c7OS, SETUP FLORIAN\u00d3POLIS , VIGILANCIA SETUP, CTFM - ILLUMINATIO ARARANGUA, CTFM - ILLUMINATIO CD, SETUP COMERCIO MATRIZ, VM - DISTRIBUIDORA ARARANGUA MATRIZ, VM - DISTRIBUIDORA CD, VM - DISTRIBUIDORA CRICIUMA, ENGECO PROJETOS E CONSTRUCOES LTDA, FFW ADMINISTRADORA DE BENS, SETUP LOCA\u00c7\u00d5ES\nData requisi\u00e7\u00e3o: 01/01/1900 at\u00e9 01/01/2900\nData atendimento: 01/01/1900 at\u00e9 01/01/2900"}],"Identificacao":"m2_estoque_atendimentos_de_requisicao","FiltrosSql":[{"Nome":"FILTROSWHERE","Valor":"WHERE IDEMPRESA = 211 AND IDFILIAL IN (333,334,335,336,387,404,520,558,339,578,340,342,343,341,344,345,346,381,389,390) AND DATA_REQ >= '01/01/1900' AND DATA_REQ <= '01/01/2900' AND DATA_ATEND >= '01/01/1900' AND DATA_ATEND <= '01/01/2900'"}]}

### Saldo de Estoque: Ajustar os filtros conforme desejado.

Link: https://extracao.useallcloud.com.br/api/v1/json?Identificacao=m2_estoque_saldo_de_estoque&FiltrosSqlQuery=[{"Nome":"ADDATA","Valor":"06/01/2026"},{"Nome":"ANQUEBRA","Valor":0},{"Nome":"FILTROSWHEREFORNEC","Valor":""},{"Nome":"FILTROSREGISTROSATIVO","Valor":" AND ITEM.ATIVO = 1 AND ALMOX.ATIVO = 1 AND ITEMALMOX.ATIVO = 1"},{"Nome":"FILTROSWHERE","Valor":" AND EXISTS(SELECT 1 FROM USE_USUARIOS_FILIAIS UFILIAIS WHERE UFILIAIS.IDEMPRESA = T.IDEMPRESA AND UFILIAIS.IDFILIAL = T.IDFILIAL AND UFILIAIS.IDUSUARIO = 2875) AND T.IDFILIAL in (336) AND T.IDALMOX IN (2571) AND T.IDITEM IN (401905) AND T.SALDODISPONIVEL > 0"}]

### Extrato de estoque: Deve ser feito a busca por item. Ajustar os filtros conforme desejado.

Link: https://extracao.useallcloud.com.br/api/v1/json?Identificacao=m2_estoque_extrato_de_estoque&FiltrosSqlQuery=[{"Nome":"filtroswhere1","Valor":" AND M2ITEM.IDITEM = 413936"},{"Nome":"filtroswhere2","Valor":" AND M2MOVTOITEM.DATA BETWEEN '01/01/2026' AND '31/01/2026' AND M2ITEM.IDITEM = 413936"},{"Nome":"dataini","Valor":"01/01/2026"}]
