import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import BytesIO

def bronze_to_silver():
    """
    Reads JSON files from the bronze bucket, transforms them to Parquet,
    and saves them to the silver bucket, partitioned by state.
    Implements logging and data quality checks for monitoring and alerting.
    """
    logger = logging.getLogger("bronze_to_silver")
    hook = S3Hook(aws_conn_id="MINIO_S3")
    bronze_bucket = "bronze"
    silver_bucket = "silver"

    # Create silver bucket if it does not exist
    if not hook.check_for_bucket(silver_bucket):
        logger.info(f"Bucket {silver_bucket} does not exist. Creating...")
        hook.create_bucket(bucket_name=silver_bucket)

    # List files in bronze bucket
    keys = hook.list_keys(bucket_name=bronze_bucket)
    if not keys:
        logger.warning("No files found in bronze bucket.")
        return

    for key in keys:
        try:
            # Read JSON from MinIO
            json_data = hook.read_key(key, bucket_name=bronze_bucket)
            df = pd.read_json(json_data)

            # Drop rows with missing state or city (optional for data quality)
            df = df.dropna(subset=['state', 'city'])

            # Data quality check: Ensure DataFrame is not empty
            if df.empty:
                logger.error(f"Empty DataFrame after cleaning for file {key}.")
                raise ValueError(f"Empty DataFrame for {key}")

            # Partition by state and save to silver bucket
            for state, group in df.groupby('state'):
                out_key = f"state={state}/breweries_{key.replace('.json','')}.parquet"
                buffer = BytesIO()
                group.to_parquet(buffer, index=False)
                buffer.seek(0)
                hook.load_bytes(buffer.getvalue(), key=out_key, bucket_name=silver_bucket, replace=True)
                logger.info(f"File {out_key} saved to silver bucket.")
        except Exception as e:
            logger.error(f"Error processing file {key}: {e}")
            raise
