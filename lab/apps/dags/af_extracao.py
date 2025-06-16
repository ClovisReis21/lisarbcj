from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone
import requests

local_tz = timezone("America/Sao_Paulo")


def executar_requisicao_extrator():
    url = "http://172.17.0.1:8000/run/extrator"
    
    try:
        response = requests.post(url)
        response.raise_for_status()
        print(f"Resposta da API: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição: {e}")
        raise


with DAG(
    dag_id='af_extracao',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule='0 0,2-23 * * *',  # Todos os horários exceto 01h
    catchup=False,
    tags=['extracao']
) as dag:

    extracao = PythonOperator(
        task_id='executar_requisicao_extrator',
        python_callable=executar_requisicao_extrator
    )
