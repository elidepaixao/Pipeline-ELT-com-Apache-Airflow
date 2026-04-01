# Pipeline ELT com Apache Airflow

Este projeto é uma atividade prática de Fundamentos de Engenharia de Dados. O objetivo foi criar uma Pipeline ELT (Extract, Load, Transform) automatizada utilizando o Apache Airflow e o Docker.

A pipeline extrai dados do famoso banco de dados de vendas Northwind (hospedado em um PostgreSQL de origem), salva as informações localmente como um arquivo CSV e, em seguida, as carrega em um segundo banco PostgreSQL (destino).

## Tecnologias Utilizadas

- Python: Linguagem principal do projeto.
- Apache Airflow: Ferramenta de orquestração de fluxo de trabalho (usando o Astronomer Astro CLI).
- Bases de Dados: Northwind (como base de dados de origem).
- PostgreSQL: Sistema Gerenciador de Banco de Dados (SGBD) para origem e destino.
- Docker e Docker Compose: Containerização dos bancos de dados e do ambiente Airflow.
- Pandas: Manipulação de dados e geração do arquivo CSV.
- Psycopg2: Biblioteca para conexão com o PostgreSQL.

## Arquitetura da Pipeline (DAG)

A pipeline foi construída com as seguintes tarefas (tasks):

1. **extract_customers**: Conecta ao PostgreSQL de origem (Porta 5433, contendo o banco Northwind), extrai os dados da tabela customers e retorna em formato JSON.
2. **save_customers_locally**: Recebe os dados da tarefa anterior via XCom (Airflow), converte para um DataFrame Pandas e salva como customers.csv dentro do container.
3. **load_customers_to_target**: Lê o arquivo customers.csv, conecta ao PostgreSQL de destino (Porta 5434), recria a tabela customers (se necessário) e insere todos os registros extraídos do Northwind.

## Como Executar o Projeto

### Pre-requisitos

- Docker e Docker Compose instalados.
- Astro CLI instalado (para rodar o Airflow).

### Passo a Passo

1. Clone o repositório:
   ```bash
   git clone https://github.com/SEU_USUARIO/pratica-fundamentos-engenharia-de-dados.git
   cd pratica-fundamentos-engenharia-de-dados

2. Suba os Bancos de Dados (PostgreSQL Origin e Target):

    docker-compose up -d

3. Inicie o Apache Airflow:

    astro dev start
4. Acesse o Airflow: Abra seu navegador no endereco gerado pelo Astro (geralmente http://localhost:8080 ou a porta informada no terminal).

    Usuario: admin
    Senha: admin
    Execute a Pipeline:

5. Na interface do Airflow, procure pela DAG first_dag.
    Ative a DAG (toggle para On).
    Clique no botao Play (Trigger DAG).

6. Verifique os Resultados: 

    Para confirmar que os dados da tabela Northwind foram carregados, acesse o banco de destino via terminal:
    docker exec -it pratica_fundamentos_engenharia_de_dados-target_db-1 psql -U postgres -d target_db

    Dentro do PostgreSQL, execute a consulta:
    SELECT * FROM customers;
    Para sair do banco, digite \q   

## Estrutura do Projeto:
    dags/first_dag.py: Codigo-fonte com a definicao da DAG e das funcoes Python.
    docker-compose.yml: Configuracao dos containers dos bancos de dados.
    requirements.txt: Dependencias do Python (Pandas e Psycopg2).