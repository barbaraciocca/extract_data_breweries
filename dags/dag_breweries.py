from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.extract import breweries_to_bronze

default_args = {
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='breweries_extract_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='breweries_to_bronze',
        python_callable=fetch_breweries
    )