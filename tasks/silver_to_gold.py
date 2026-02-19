import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import BytesIO

def silver_to_gold():
    """
    Reads Parquet files from the silver bucket, aggregates brewery counts by state and type,
    and saves the result to the gold bucket in MinIO.
    """
    logger = logging.getLogger("silver_to_gold")
    hook = S3Hook(aws_conn_id="MINIO_S3")
    silver_bucket = "silver"
    gold_bucket = "gold"

    # Create bucket if it does not exist
    if not hook.check_for_bucket(gold_bucket):
        logger.info(f"Bucket {gold_bucket} does not exist. Creating...")
        hook.create_bucket(bucket_name=gold_bucket)

    # List files in silver bucket
    keys = hook.list_keys(bucket_name=silver_bucket)
    if not keys:
        logger.warning("No files found in silver bucket.")
        return

    frames = []
    for key in keys:
        try:
            obj = hook.get_key(key, bucket_name=silver_bucket)
            parquet_bytes = BytesIO(obj.get()['Body'].read())
            df = pd.read_parquet(parquet_bytes)
            frames.append(df)
            logger.info(f"File {key} read from silver bucket.")
        except Exception as e:
            logger.error(f"Error reading/parquet file {key}: {e}")
            raise

    # Ensure DataFrames is not empty
    if not frames:
        logger.error("No DataFrames loaded from silver bucket files.")
        raise ValueError("No DataFrames loaded.")

    full_df = pd.concat(frames, ignore_index=True)

    # Ensure full_df is not empty
    if full_df.empty:
        logger.error("Final DataFrame is empty after concatenation.")
        raise ValueError("Final DataFrame is empty.")

    agg_df = full_df.groupby(['state', 'brewery_type']).size().reset_index(name='count')
    logger.info("Aggregated DataFrame created.")

    buffer = BytesIO()
    agg_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    hook.load_bytes(buffer.getvalue(), key="aggregated_breweries.parquet", bucket_name=gold_bucket, replace=True)
    logger.info("File aggregated_breweries.parquet saved to gold bucket.")
