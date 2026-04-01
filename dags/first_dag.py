from airflow import DAG

from airflow.operators.python import PythonOperator 
from datetime import datetime # define a data de inicio da DAG

import pandas as pd # ler dados do banco, salvar como CSV, ler o CSV
import psycopg2 # conecta ao banco de dados postgreSQL


## Configurações de conexão

# banco de origem
SOURCE_DB = {
    "host": "host.docker.internal",
    "port": 5433,
    "database": "source_db",
    "user": "postgres",
    "password": "postgres"
}

# banco de destino
TARGET_DB = {
    "host": "host.docker.internal",
    "port": 5434,
    "database": "target_db",
    "user": "postgres",
    "password": "postgres"

}

# Define o caminho onde o arquivo CSV será salvo. Esse é o caminho dentro do container Docker do Airflow

CSV_PATH = "/usr/local/airflow/dags/customers.csv"

## EXTRAÇÃO:

def extract_customers(): # função executada como primeira tarefa da pipelne.
    print("Conectando ao source_db (Northwind)...") #Mostra uma mensagem no log do Airflow.

    conn = psycopg2.connect(    # abrindo uma conexão com o POSTGRESQL
        host=SOURCE_DB["host"],
        port=SOURCE_DB["port"],
        database=SOURCE_DB["database"],
        user=SOURCE_DB["user"],
        password=SOURCE_DB["password"]
    )

    query = "SELECT * FROM customers;" 
    df = pd.read_sql(query, conn) #lendo os dados do banco e armazenando em um DataFrame do Pandas
    conn.close()

    print(f"Extração concluída! {len(df)} registros.") 
    print(df.head()) #mostra as 5 primeiras linhas do DataFrame para verificar os dados extraídos
    return df.to_json() #converte o DataFrame inteiro para uma string JSON

## SALVAR 

# **context: parametro do Airflow / ** receba todos os argumentos extras como um dicionário.
# O Airflow automaticamente passa informações da execução nesse context
# Precisamos dele para acessar o XCom (dados da task anterior)
def save_customers_locally(**context): 
    ti = context['ti'] #pegando o Task Instance do Airflow para acessar o XCom
    customers_json = ti.xcom_pull(task_ids='extract_customers') #puxa os dados que a task anterior retornou

    df = pd.read_json(customers_json) #converte a string JSON de volta para um DataFrame do Pandas
    df.to_csv(CSV_PATH, index=False) #salva o DataFrame como um arquivo CSV

## CARREGAR

def load_customers_to_target(): # define a função da terceira e última task
    print("Carregando dados para target_db...")

    df = pd.read_csv(CSV_PATH)

    conn = psycopg2.connect( 
        host=TARGET_DB["host"],
        port=TARGET_DB["port"],
        database=TARGET_DB["database"],
        user=TARGET_DB["user"],
        password=TARGET_DB["password"]
    )
    cursor = conn.cursor() # "digita" os comandos SQL

    # Criar a tabela customers no banco de destino:

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id VARCHAR(5) NOT NULL PRIMARY KEY,
        company_name VARCHAR(40) NOT NULL,
        contact_name VARCHAR(30),
        contact_title VARCHAR(30),
        address VARCHAR(60),
        city VARCHAR(15),   
        region VARCHAR(15),
        postal_code VARCHAR(10),
        country VARCHAR(15),
        phone VARCHAR(24),
        fax VARCHAR(24)
        
    );
""")             

    cursor.execute("TRUNCATE TABLE customers;") #limpa a tabela antes de inserir os novos dados

    for _, row in df.iterrows(): #iterando sobre as linhas do DataFrame e inserindo os dados na tabela
        cursor.execute("""
            INSERT INTO customers (
                       customer_id, company_name, contact_name,
                       contact_title, address, city, region,
                       postal_code, country, phone, fax
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 """, (
            row['customer_id'],
            row['company_name'],
            row['contact_name'],
            row['contact_title'],
            row['address'],
            row['city'],
            row['region'],
            row['postal_code'],
            row['country'],
            row['phone'],
            row['fax']
        ))
    conn.commit() #confirma as  mudanças no banco de dados
    cursor.close()
    conn.close()
    print("Carga concluída!")

## Definição da DAG

with DAG(
    dag_id='first_dag',
    start_date=datetime(2024, 6, 1), 
    schedule=None,
    catchup=False
) as dag:
    
    # Task 1: Extração dos dados
    extract_task = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers
    )

    # Task 2: Salvar os dados localmente como CSV
    save_task = PythonOperator(
        task_id='save_customers_locally',
        python_callable=save_customers_locally,
    )

    # Task 3: Carregar os dados para o banco de destino
    load_task = PythonOperator(
        task_id='load_customers_to_target',
        python_callable=load_customers_to_target
    )

    # Definindo a ordem de execução das tasks
    extract_task >> save_task >> load_task