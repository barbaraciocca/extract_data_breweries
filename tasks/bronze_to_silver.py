# tasks/transform.py

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import BytesIO

def bronze_to_silver():
    """
    Lê arquivos JSON do bucket bronze, transforma em Parquet
    e salva no bucket silver, particionando por estado.
    """
    hook = S3Hook(aws_conn_id="MINIO_S3")
    bronze_bucket = "bronze"
    silver_bucket = "silver"

    # Cria bucket silver se não existir
    if not hook.check_for_bucket(silver_bucket):
        hook.create_bucket(bucket_name=silver_bucket)

    # Lista arquivos no bucket bronze
    keys = hook.list_keys(bucket_name=bronze_bucket)
    if not keys:
        return

    for key in keys:
        # Lê JSON do MinIO
        json_data = hook.read_key(key, bucket_name=bronze_bucket)
        df = pd.read_json(json_data)

        # Seleciona colunas relevantes
        df = df[['id', 'name', 'brewery_type', 'city', 'state', 'country', 'postal_code']]
        df = df.dropna(subset=['state', 'city'])

        # Particiona por estado
        for state, group in df.groupby('state'):
            out_key = f"state={state}/breweries_{key.replace('.json','')}.parquet"
            buffer = BytesIO()
            group.to_parquet(buffer, index=False)
            buffer.seek(0)
            hook.load_bytes(buffer.getvalue(), key=out_key, bucket_name=silver_bucket, replace=True)
