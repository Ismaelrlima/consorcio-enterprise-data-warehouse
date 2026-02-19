# Consórcio Data Warehouse Pipeline

Pipeline de Engenharia de Dados desenvolvido em Python com arquitetura
em camadas (Bronze → Silver → Gold), modelagem dimensional em Star
Schema e carga estruturada em PostgreSQL.

Este projeto demonstra aplicação prática de princípios fundamentais de
Data Engineering: ingestão estruturada, transformação semântica,
modelagem dimensional, idempotência, separação de responsabilidades e
execução containerizada.

------------------------------------------------------------------------

## Visão Geral

O pipeline realiza:

-   Extração de dados via API
-   Padronização e tratamento de dados
-   Geração de hash para controle de duplicidade
-   Construção de dimensões com surrogate keys
-   Criação de tabela fato
-   Carga estruturada em Data Warehouse PostgreSQL

A arquitetura foi projetada com foco em organização, reprocessamento
seguro e evolução para cenários produtivos.

------------------------------------------------------------------------

## Arquitetura

O projeto adota separação clara de responsabilidades seguindo o padrão:

Bronze → Silver → Gold

### Bronze (Ingestão)

-   Consumo da API
-   Conversão para DataFrame
-   Preservação do dado bruto

### Silver (Tratamento)

-   Padronização de colunas (snake_case)
-   Conversão de tipos
-   Tratamento de datas
-   Normalização de valores numéricos
-   Geração de `row_hash`
-   Preparação para modelagem dimensional

### Gold (Modelagem Dimensional)

-   Construção de dimensões
-   Geração de surrogate keys (SK)
-   Montagem da tabela fato
-   Relacionamentos via chaves substitutas

------------------------------------------------------------------------

## Modelo Dimensional

### Dimensões

dim_vendedor\
- sk_vendedor (PK)\
- id_pessoa_vendedor (natural key)\
- vendedor

dim_administradora\
- sk_administradora (PK)\
- id_administradora (natural key)\
- administradora

dim_tempo\
- sk_tempo (PK)\
- data_venda\
- ano\
- mes\
- dia\
- dia_da_semana\
- trimestre

### Fato

fato_vendas\
- sk_vendedor (FK)\
- sk_administradora (FK)\
- sk_tempo (FK)\
- valor_credito\
- valor_primeira_parcela\
- row_hash

A tabela fato mantém exclusivamente métricas e chaves substitutas,
seguindo boas práticas de modelagem dimensional.

------------------------------------------------------------------------

## Idempotência e Controle de Duplicidade

O pipeline gera um `row_hash` com base nos atributos relevantes da
venda.

Isso permite:

-   Reexecução segura
-   Evitar duplicidade de registros
-   Garantir consistência em cargas repetidas
-   Base estrutural para futura carga incremental

------------------------------------------------------------------------

## Logging e Observabilidade

O projeto utiliza logging estruturado para:

-   Monitoramento da execução
-   Rastreabilidade de falhas
-   Auditoria de cargas

Os logs são persistidos em arquivo (`pipeline.log`) e podem ser
integrados a soluções externas de observabilidade.

------------------------------------------------------------------------

## Estrutura do Projeto

📦 src\
┣ 📂 bronze\
┃ ┗ 📜 bronze_layer.py\
┣ 📂 gold\
┃ ┣ 📜 dimensions.py\
┃ ┣ 📜 fact.py\
┃ ┣ 📜 gold_pipeline.py\
┃ ┗ 📜 incremental.py\
┣ 📂 ingestion\
┃ ┗ 📜 microwork_api.py\
┣ 📂 load\
┃ ┗ 📜 dev_load_gold.py\
┣ 📂 logs\
┃ ┗ 📜 pipeline.log\
┣ 📂 silver\
┃ ┗ 📜 silver_layer.py\
┣ 📂 sql\
┃ ┗ 📜 create_dw.sql\
┣ 📂 utils\
┃ ┣ 📜 extrator.py\
┃ ┗ 📜 logging_config.py\
┣ 📜 main.py\
┗ 📜 requirements.txt

------------------------------------------------------------------------

## Tecnologias Utilizadas

-   Python 3.10+
-   Pandas
-   SQLAlchemy
-   PostgreSQL
-   Docker
-   Logging padrão da biblioteca Python

------------------------------------------------------------------------

## Execução (Ambiente Containerizado)

### 1. Clonar o repositório

git clone https://github.com/Ismaelrlima/consorcio-enterprise-data-warehouse.git
cd consorcio-pipeline

### 2. Criar arquivo .env

Criar um arquivo `.env` na raiz do projeto:

DB_HOST=localhost\
DB_PORT=5432\
DB_USER=usuario\
DB_PASSWORD=senha\
DB_NAME=banco

### 3. Build da imagem Docker

docker build -t consorcio-pipeline .

### 4. Executar container

docker run --env-file .env consorcio-pipeline

A execução do pipeline ocorre automaticamente via `main.py` dentro do
container.

------------------------------------------------------------------------

## Decisões Técnicas

-   Arquitetura em camadas para escalabilidade e manutenção
-   Uso de surrogate keys para desacoplamento de chaves naturais
-   Star Schema para otimização analítica
-   Hashing para garantir idempotência
-   Separação clara entre ingestão, transformação e carga
-   Containerização para reprodutibilidade de ambiente

------------------------------------------------------------------------

## Considerações de Performance

-   Uso de operações vetorizadas com Pandas
-   Organização em camadas para evitar reprocessamento desnecessário
-   Estrutura preparada para evolução para processamento incremental

------------------------------------------------------------------------

## Próximos Passos Técnicos

-   Implementação formal de carga incremental
-   SCD (Slowly Changing Dimensions)
-   Índices e constraints automáticas no DW
-   Docker Compose para orquestração com PostgreSQL
-   Orquestração com Airflow
-   Testes automatizados

------------------------------------------------------------------------

## Conceitos Demonstrados

-   Data Warehousing
-   Modelagem Dimensional
-   Star Schema
-   Surrogate Keys
-   Idempotência em pipelines
-   Arquitetura modular
-   Separação de responsabilidades
-   Containerização de aplicações

------------------------------------------------------------------------

Projeto desenvolvido como prática aplicada de Engenharia de Dados com
foco em organização arquitetural, modelagem analítica e boas práticas
alinhadas ao mercado.
