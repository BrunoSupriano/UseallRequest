# 📶 Extração de dados da Useall

- Executavel python responsavel por extrair dados da api da useall, inserir em um banco postgres passando pela camada staging, silver e gold sendo gold a camada com dados finais e colunas adicionais implementadas

# 👨🏽‍💻 Stack Utilizada

- 🐍 **[Python](https://www.python.org/downloads/release/python-3132/)** – Extração, transformação e carga de dados (ETL)  
- 🎲 **[Pandas](https://pandas.pydata.org/)** – Manipulação, limpeza e transformação de DataFrames  
- 🐘 **[PostgreSQL](https://www.postgresql.org/)** – Armazenamento estruturado em camadas (staging, silver, gold)  
- 📊 **[Power BI (Desktop)](https://powerbi.microsoft.com/desktop/)** – Visualização e análise de dados  

# 🚀 Como Executar o projeto

### ⚠️ Dicas

- Verifique se o Docker está ativo antes de subir os containers.

- Confirme as variáveis de ambiente do banco (usuário, senha, host, porta) antes de rodar o script Python.

- A camada gold é a que contém os dados finais prontos para análise e visualização no Power BI.

### 🛠️ .env (API-Key e Config Banco)

> **-** Caso seja um contribuidor do Projeto:

- Utilize o arquivo ".env.example"(não renomeie) para criar um **novo arquivo** chamado ".env" para não correr o risco de commitar a chave de API.

- Substitua o "USEALL_TOKEN=seu_token_aqui" do arquivo .env com a sua chave de API.

> **-** Caso seja apenas um visualizador:

- Renomeie o arquivo ".env.example" para ".env"

- Substitua o "USEALL_TOKEN=seu_token_aqui" do arquivo .env com a sua chave de API.


## 🐘 Postgres (Docker 🐋)

- ⚠️ Este comando apaga e reinicia o container se existir.
- ⚠️ Execute a partir da Raiz do sistema ou remova o caminho relativo "docker/".

```bash
docker-compose -f docker/docker-compose.yml down -v
docker-compose -f docker/docker-compose.yml up -d
```
## 🐍 Python

#### 📚 Bibliotecas

```bash
pip install -r .\notebooks\requirements.txt
```
#### ↘️ Pipeline

```bash
python .\notebooks\modelobanco-test.py
```

## 📶 API USEAL STATUS DE EXTRAÇÃO

*-* Bases Simples

* [X] itens
* [X] unidades
* [X] segmentos
* [X] cidades
* [X] solcompra
* [X] filiais
* [X] empresas
* [X] expedição

*-* Bases Complexas

* [X] Requisição de Material - 679071 linhas
* [X] Saldo de Estoque - 132574 linhas
* [X] Atendimento de Requisições - 519077 linhas

*-* ⚠️ Bases Pendentes ⚠️

* [ ] Extrato de Estoque - Solicitado apoio Useall - sem documentação de API
* [ ] Custo de Estoque - Solicitado apoio Useall - sem documentação de API


# Testes e Estudo de relacionamento

### 📊 Grafo de arvore de relacionamento

![Grafo de Relacionamentos](../notebooks/tests/grafo_relacionamentos.png)

### 📊 Dados relacionados com porcentagem 

[Excel - Relacionamentos Sugeridos](../notebooks/tests/relacionamentos_sugeridos.xlsx)