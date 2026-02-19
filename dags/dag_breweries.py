"""
Airflow DAG for the ETL pipeline of brewery data.

This DAG runs daily (when turned on) and orchestrates three steps:
1. Extracts brewery data from an external API to the bronze bucket.
2. Partitions the data by state, saving data as Parquet files in the silver bucket.
3. Aggregates brewery counts by state and type, saving the result as a Parquet file in the gold bucket.

All data is stored in MinIO.
Each step is a Python task. Failures are logged via a callback.
Task order: bronze_layer → silver_layer → gold_layer.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.api_to_bronze import breweries_to_bronze
from tasks.bronze_to_silver import bronze_to_silver
from tasks.silver_to_gold import silver_to_gold

def failure_callback(context):
    """
    Callback function for task failure, and logs the error.
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = context.get('task').task_id
    exception = context.get('exception')
    print(f"[ALERT] Task {task_id} in DAG {dag_id} failed: {exception}")

with DAG(
    dag_id="ETL-breweries",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_bronze = PythonOperator(
        task_id="bronze_layer",
        python_callable=breweries_to_bronze,
        on_failure_callback=failure_callback
    )

    task_silver = PythonOperator(
        task_id="silver_layer",
        python_callable=bronze_to_silver,
        on_failure_callback=failure_callback
    )

    task_gold = PythonOperator(
        task_id="gold_layer",
        python_callable=silver_to_gold,
        on_failure_callback=failure_callback
    )

    task_bronze >> task_silver >> task_gold
