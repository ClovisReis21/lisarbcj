from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone
import requests

local_tz = timezone("America/Sao_Paulo")

def executar_requisicao_ingesta():
    url = "http://172.17.0.1:8000/run/ingesta"
    
    try:
        response = requests.post(url)
        response.raise_for_status()
        print(f"Resposta da API: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição: {e}")
        raise

def executar_requisicao_batch():
    url = "http://172.17.0.1:8000/run/batch"
    
    try:
        response = requests.post(url)
        response.raise_for_status()
        print(f"Resposta da API: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição: {e}")
        raise

with DAG(
    dag_id='af_ingestao_batch',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule_interval='0 1 * * *',  # Executa todo dia às 01:00
    # schedule_interval='*/10 * * * *',  # Executa a cada 10 minutos
    catchup=False,
    tags=['ingestao', 'batch']
) as dag:

    ingestao = PythonOperator(
        task_id='executar_requisicao_ingesta',
        python_callable=executar_requisicao_ingesta
    )

    batch = PythonOperator(
        task_id='executar_requisicao_batch',
        python_callable=executar_requisicao_batch
    )

    ingestao >> batch

