from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone
import requests

local_tz = timezone("America/Sao_Paulo")


def executar_requisicao_streaming():
    url = "http://172.17.0.1:8000/run/speed"
    
    try:
        response = requests.post(url)
        response.raise_for_status()
        print(f"Resposta da API: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição: {e}")
        raise


with DAG(
    dag_id='af_streaming',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule_interval=None,  # Não agenda automaticamente
    catchup=False,
    tags=['af_streaming']

) as dag:

    streaming = PythonOperator(
        task_id='executar_requisicao_streaming',
        python_callable=executar_requisicao_streaming
    )
