from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import requests
import logging
import boto3
import io
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from credentials import (GOOGLE_SERVICE_ACCOUNT_JSON, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME,
                         GCP_PROJECT_ID, GCP_BQ_DATASET_LOCATION, user, host, password, port, database, tabela,
                         GCP_BQ_TABLE_ID, GCP_BQ_DATASET_ID)

# Obter conexão com GCP e AWS

# GCP
credentials = Credentials.from_service_account_info(GOOGLE_SERVICE_ACCOUNT_JSON)
client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID, location=GCP_BQ_DATASET_LOCATION)

# AWS
boto3.setup_default_session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION_NAME
)

# Criar funções que serão utilizadas na DAG
def extract_api_employees():
    """
    Esta função tem o objetivo de acessar o endpoint https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior
    que recebe como parâmetro um id que atualmente varia de 1 a 9. Para caso o número de funcionários aumente ou diminua,
    a função realizará um loop para puxar todos os funcionários, independente de quantos sejam.
    O resultado é um arquivo csv representando o snapshot do dia, armazenado no bucket staging-bix-test, na pasta de funcionarios.
    """

    logging.info("iniciando extração de dados da api de funcionarios")
    id = []
    name = []

    employee_id = 0
    while True:
        employee_id += 1
        endpoint = f"https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={employee_id}"
        response = requests.get(endpoint)
        if response.status_code != 200:
            logging.warning(f"error while trying to access endpoint for id: {employee_id}: {response.status_code}")
            break

        n = str(response.content.decode('utf-8'))
        if n == "The argument is not correct":
            logging.info(f"data extraction finished. last id: {employee_id}")
            break
        else:
            id.append(employee_id)
            name.append(n)
    ldf = list(zip(id, name))
    df_employees = pd.DataFrame(ldf, columns=['id', 'name'])
    employees_csv = df_employees.to_csv(index=False)
    s3 = boto3.client('s3')
    bucket_name = "staging-bix-test"
    s3.put_object(Body=employees_csv.encode('utf-8'), Bucket=bucket_name, Key=f"employees/employees_{datetime.today().strftime('%m-%d-%Y')}.csv")

    logging.info("Data uploaded to S3 bucket")

    return None
def extract_sql_sales():
    """
    Esta função tem como objetivo extrair os dados da tabela de vendas de um banco de dado Postgres.
    O resultado é um arquivo csv representando o snapshot do dia, armazenado no bucket staging-bix-test, na pasta de vendas.
    """
    logging.info("iniciando extração de dados do db de vendas")

    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

    df_sales = pd.read_sql(f"select * from {tabela}", engine)

    sales_csv = df_sales.to_csv(index=False)

    s3 = boto3.client('s3')
    bucket_name = "staging-bix-test"
    s3.put_object(Body=sales_csv.encode('utf-8'), Bucket=bucket_name, Key=f"sales/sales_{datetime.today().strftime('%m-%d-%Y')}.csv")

    logging.info("Data uploaded to S3 bucket")

    return None
def extract_parquet_categories():
    """
    Esta função tem como objetivo extrair os dados de categoria que estão num arquivo parquet.
    O resultado é um arquivo csv representando o snapshot do dia, armazenado no bucket staging-bix-test, na pasta de categorias.
    """
    logging.info("iniciando extração de dados do parquet de categorias")

    df_categories = pd.read_parquet('https://storage.googleapis.com/challenge_junior/categoria.parquet')
    df_categories = df_categories.rename(columns={'id': 'id_categoria'})

    categories_csv = df_categories.to_csv(index=False)

    s3 = boto3.client('s3')
    bucket_name = "staging-bix-test"
    s3.put_object(Body=categories_csv.encode('utf-8'), Bucket=bucket_name, Key=f"categories/categories_{datetime.today().strftime('%m-%d-%Y')}.csv")

    logging.info("Data uploaded to S3 bucket")

    return None
def join_dfs():
    """
    Esta função tem o objetivo de juntar os arquivos gerados pelas tasks anteriores desta dag, apagando colunas desnecessárias.
    O resultado é um arquivo csv representando o snapshot do dia, armazenado no bucket staging-bix-test, na pasta de tabelas juntas.
    """
    logging.info("executando junção das tabelas")

    employees_csv_path = f"employees/employees_{datetime.today().strftime('%m-%d-%Y')}.csv"
    categories_csv_path = f"categories/categories_{datetime.today().strftime('%m-%d-%Y')}.csv"
    sales_csv_path = f"sales/sales_{datetime.today().strftime('%m-%d-%Y')}.csv"

    s3 = boto3.client('s3')
    bucket_name = "staging-bix-test"

    employees_csv_obj = s3.get_object(Bucket=bucket_name, Key=employees_csv_path)
    employees_data = employees_csv_obj['Body'].read()
    df_employees = pd.read_csv(io.BytesIO(employees_data), encoding='utf-8')

    categories_csv_obj = s3.get_object(Bucket=bucket_name, Key=categories_csv_path)
    categories_data = categories_csv_obj['Body'].read()
    df_categories = pd.read_csv(io.BytesIO(categories_data), encoding='utf-8')

    sales_csv_obj = s3.get_object(Bucket=bucket_name, Key=sales_csv_path)
    sales_data = sales_csv_obj['Body'].read()
    df_sales = pd.read_csv(io.BytesIO(sales_data), encoding='utf-8')

    df_sales_categories = df_sales.merge(df_categories, left_on='id_categoria', right_on='id_categoria')
    df_joined = df_sales_categories.merge(df_employees, left_on='id_funcionario', right_on='id')

    df_joined = df_joined.rename(columns={'nome_categoria': 'categoria', 'name': 'funcionario'})

    df_joined['data_venda'] = pd.to_datetime(df_joined['data_venda'])

    df_joined['venda'] = df_joined['venda'].astype(float)

    df_joined = df_joined.drop(columns=['id', 'id_funcionario', 'id_categoria'], axis=1)

    joined_df_csv_path = f"joined/joined_{datetime.today().strftime('%m-%d-%Y')}.csv"

    joined_df_csv = df_joined.to_csv(index=False)

    s3.put_object(Body=joined_df_csv.encode('utf-8'), Bucket=bucket_name, Key=joined_df_csv_path)
def load_df_bq():
    """
    Esta função tem o objetivo de carregar a tabela que foi resultado da junção das três tabelas de vendas, categorias e funcionários,
    num armazém de dados no Big Query.
    O resultado é a atualização da tabela bix_sales no Big Query.
    """
    logging.info("carregando tabela unificada para bigquery")

    joined_csv_path = f"joined/joined_{datetime.today().strftime('%m-%d-%Y')}.csv"

    s3 = boto3.client('s3')
    bucket_name = "staging-bix-test"

    joined_csv_obj = s3.get_object(Bucket=bucket_name, Key=joined_csv_path)
    joined_data = pd.read_csv(joined_csv_obj['Body'])
    df_joined = pd.DataFrame(joined_data)
    df_joined['data_venda'] = pd.to_datetime(df_joined['data_venda'])

    dataset_ref = bigquery.DatasetReference(project=GCP_PROJECT_ID, dataset_id=GCP_BQ_DATASET_ID)
    table_ref = bigquery.TableReference(dataset_ref, GCP_BQ_TABLE_ID)

    df_joined.to_gbq(destination_table=GCP_BQ_TABLE_ID, project_id=GCP_PROJECT_ID, location=GCP_BQ_DATASET_LOCATION, credentials=credentials, if_exists="replace")

    print(f"Data loaded to BigQuery table: {table_ref.table_id}")

with DAG("bix_dag",
         start_date=datetime(2024,4,26),
         schedule_interval="0 14 * * *",
         catchup=False,
         default_args={
             'start_date': datetime(2024, 4, 23),
             'owner': 'Rodrigo',
         }
         ) as dag:

    logging.info("iniciando DAG bix_dag")

    extract_api = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api_employees,
        dag=dag
    )

    extract_sql = PythonOperator(
        task_id="extract_sql",
        python_callable=extract_sql_sales,
        dag=dag
    )

    extract_parquet = PythonOperator(
        task_id="extract_parquet",
        python_callable=extract_parquet_categories,
        dag=dag
    )

    merge_dfs = PythonOperator(
        task_id="merge_dfs",
        python_callable=join_dfs,
        dag=dag,
        op_kwargs={
            'df_sales': '{{ ti.xcom_pull(task_ids="extract_sql") }}',
            'df_employees': '{{ ti.xcom_pull(task_ids="extract_api") }}',
            'df_categories': '{{ ti.xcom_pull(task_ids="extract_parquet") }}',
        }
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_df_bq,
        dag=dag
    )

    extract_api >> merge_dfs
    extract_sql >> merge_dfs
    extract_parquet >> merge_dfs
    merge_dfs >> load
