from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.extract import breweries_to_bronze, bronze_to_silver, silver_to_gold


with DAG(
    dag_id="breweries_data_lake",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_bronze = PythonOperator(
        task_id="bronze_layer",
        python_callable=breweries_to_bronze
    )

    task_silver = PythonOperator(
        task_id="silver_layer",
        python_callable=bronze_to_silver
    )

    task_gold = PythonOperator(
        task_id="gold_layer",
        python_callable=silver_to_gold
    )

    task_bronze >> task_silver >> task_gold
