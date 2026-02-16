import requests
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def breweries_to_bronze(state=None, brewery_type=None):
    """
    Busca dados da API Open Brewery DB e salva no bucket bronze do MinIO.
    
    Parâmetros opcionais:
    - state: filtra por estado
    - brewery_type: filtra por tipo de cervejaria
    """
    hook = S3Hook(aws_conn_id="MINIO_S3")
    bucket_name = "bronze"

    # Cria bucket se não existir
    if not hook.check_for_bucket(bucket_name):
        hook.create_bucket(bucket_name=bucket_name)

    page = 1
    per_page = 200  # máximo permitido pela API
    all_data = []

    while True:
        url = f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}"
        if state:
            url += f"&by_state={state.replace(' ', '_')}"
        if brewery_type:
            url += f"&by_type={brewery_type}"

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if not data:
            break

        all_data.extend(data)
        page += 1

    # Salva no MinIO
    file_key = f"breweries_{datetime.now().date()}.json"
    hook.load_string(
        string_data=json.dumps(all_data, indent=2),
        key=file_key,
        bucket_name=bucket_name,
        replace=True
    )
