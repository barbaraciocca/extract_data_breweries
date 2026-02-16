# tasks/transform.py

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import BytesIO


def silver_to_gold():
    """
    Lê todos os Parquet do bucket silver, agrega por state e brewery_type,
    e salva no bucket gold.
    """
    hook = S3Hook(aws_conn_id="MINIO_S3")
    silver_bucket = "silver"
    gold_bucket = "gold"

    if not hook.check_for_bucket(gold_bucket):
        hook.create_bucket(bucket_name=gold_bucket)

    keys = hook.list_keys(bucket_name=silver_bucket)
    if not keys:
        return

    frames = []
    for key in keys:
        parquet_bytes = hook.read_key(key, bucket_name=silver_bucket, binary=True)
        df = pd.read_parquet(BytesIO(parquet_bytes))
        frames.append(df)

    full_df = pd.concat(frames, ignore_index=True)

    agg_df = full_df.groupby(['state', 'brewery_type']).size().reset_index(name='count')

    buffer = BytesIO()
    agg_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    hook.load_bytes(buffer.getvalue(), key="aggregated_breweries.parquet", bucket_name=gold_bucket, replace=True)
