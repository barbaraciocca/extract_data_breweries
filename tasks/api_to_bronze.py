import requests
import json
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def breweries_to_bronze(state=None, brewery_type=None):
    """
    Fetches data from the Open Brewery DB API and saves it to the bronze bucket in MinIO.

    Optional parameters:
    - state: filter by state
    - brewery_type: filter by brewery type
    """
    logger = logging.getLogger("api_to_bronze")
    hook = S3Hook(aws_conn_id="MINIO_S3")
    bucket_name = "bronze"

    # Create bucket if it does not exist
    if not hook.check_for_bucket(bucket_name):
        logger.info(f"Bucket {bucket_name} does not exist. Creating...")
        hook.create_bucket(bucket_name=bucket_name)

    page = 1
    per_page = 200
    all_data = []

    while True:
        url = f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}"
        if state:
            url += f"&by_state={state.replace(' ', '_')}"
        if brewery_type:
            url += f"&by_type={brewery_type}"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Page {page} loaded from API with {len(data)} records.")
        except Exception as e:
            logger.error(f"Error fetching data from API on page {page}: {e}")
            raise

        if not data:
            break

        all_data.extend(data)
        page += 1

    # Ensure all_data is not empty
    if not all_data:
        logger.error("No data returned from API.")
        raise ValueError("No data returned from API.")

    # Save to MinIO
    file_key = f"breweries_{datetime.now().date()}.json"
    try:
        hook.load_string(
            string_data=json.dumps(all_data, indent=2),
            key=file_key,
            bucket_name=bucket_name,
            replace=True
        )
        logger.info(f"File {file_key} saved to bronze bucket.")
    except Exception as e:
        logger.error(f"Error saving file {file_key} to bronze bucket: {e}")
