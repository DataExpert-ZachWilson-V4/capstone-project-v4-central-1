# Simple DAG with TaskFlow API
import os
import logging
from datetime import datetime
import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import polars as pl

root_s3_sneakers_key = "central1/sneakers_catalog/"
s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")

@dag(
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
    catchup=False)
def load_sneakers_catalog_data_s3():
    """Load Sneakers Catalog Data to S3"""

    @task()
    def cleanup_s3():
        # Cleanup S3 bucket path "central1/sneakers_catalog/" before loading new data.
        aws_conn_id = "aws_dataexpert_conn"
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        # Get all kets with prefix "root_s3_sneakers_key".
        keys = s3_hook.list_keys(bucket_name=s3_bucket, prefix=root_s3_sneakers_key)
        s3_hook.delete_objects(bucket=s3_bucket, keys=keys)

    @task()
    def convert_json_to_parquet():
        # Convert JSON Files to Parquet using POLARS
        # This task should convert all JSON files in the S3 bucket path "central1/sneakers_catalog/JSON" to Parquet format and write to "central1/sneakers_catalog/PARQUET".

        sneakers_catalog_json = os.path.join("include", root_s3_sneakers_key, "JSON")
        sneakers_catalog_parquet = os.path.join("include", root_s3_sneakers_key, "PARQUET")

        for root, dirs, files in os.walk(sneakers_catalog_json):
            for file in files:
                local_file_path = os.path.join(root, file)
                local_file_parquet = local_file_path.replace(sneakers_catalog_json, sneakers_catalog_parquet).replace(".json", ".parquet")
                df = pl.read_json(local_file_path)
                # Write folder if not exists
                if not os.path.exists(os.path.dirname(local_file_parquet)):
                    os.makedirs(os.path.dirname(local_file_parquet))
                df.write_parquet(local_file_parquet)
                logging.info(
                    f"File {local_file_path} successfully converted to {local_file_parquet}"
                )

    
    @task()
    def load_s3():
        aws_conn_id = "aws_dataexpert_conn"
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        
        # Load all files from all folders in in "root_s3_sneakers_key" PARQUET to S3
        # For example, if root_s3_sneakers_key is "central1/sneakers_catalog/PARQUET",
        # then all files inside all folders should be loaded keeping the same folder structure.

        local_sneakers_catalog_parquet = os.path.join("include", root_s3_sneakers_key, "PARQUET")
        for root, dirs, files in os.walk(local_sneakers_catalog_parquet):
            for file in files:
                local_file_path = os.path.join(root, file)
                s3_key = local_file_path.replace("include/", "").replace(s3_bucket, "")
                s3_hook.load_file(
                    local_file_path,
                    s3_key,
                    bucket_name=s3_bucket,
                    replace=True,
                )
                s3_path = f"s3://{s3_bucket}/{s3_key}"
                logging.info(
                    f"File {local_file_path} successfully uploaded to {s3_path}"
                )

    cleanup_s3() >> convert_json_to_parquet() >> load_s3()

load_sneakers_catalog_data_s3()