from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.api_to_bronze import breweries_to_bronze
from tasks.bronze_to_silver import bronze_to_silver
from tasks.silver_to_gold import silver_to_gold

def failure_callback(context):
    """
    Callback function for task failure.
    Logs the error and can be extended to send alerts (email, Slack, etc.).
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
