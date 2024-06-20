# Simple DAG with TaskFlow API
import os
import logging
from datetime import datetime
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.central1.utils.glue_job_submission import create_glue_job

root_s3_sneakers_key = "central1/sneakers_catalog/PARQUET/"
root_s3_script_key = "central1/scripts/clean_sneakers_catalog_data.py"

S3_BUCKET = Variable.get("AWS_S3_BUCKET_TABULAR")

@dag(
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
    catchup=False)
def clean_sneakers_catalog_data():
    """Load Sneakers Catalog Data to S3"""
    
    @task()
    def load_script_s3():
        aws_conn_id = "aws_dataexpert_conn"
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        
        # Load all files from all folders in in "root_s3_sneakers_key" to S3
        # For example, if root_s3_sneakers_key is "central1/sneakers_catalog/",
        # then all files inside all folders should be loaded keeping the same folder structure.

        local_script_path = os.path.join("include", root_s3_script_key)
        s3_hook.load_file(
            local_script_path,
            root_s3_script_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
        s3_path = f"s3://{S3_BUCKET}/{root_s3_script_key}"
        logging.info(
            f"Script {local_script_path} successfully uploaded to {s3_path}"
        )
        return s3_path

    @task()
    def submit_glue_job(s3_path):
        job_name = "clean_sneakers_catalog_data"
        script_path= s3_path
        aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
        aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
        tabular_credential = Variable.get("TABULAR_CREDENTIAL")
        catalog_name = Variable.get("CATALOG_NAME")
        aws_region = Variable.get("AWS_GLUE_REGION")
        description= "Testing Job Spark"
        arguments = {
                "--ds": "{{ ds }}",
                "--output_table": "jsgomez14.sneakers_catalog_cleaned",
                "--sneakers_catalog": root_s3_sneakers_key,
                "--s3_bucket": S3_BUCKET,
            }

        create_glue_job(
            job_name,
            script_path,
            arguments,
            aws_access_key_id,
            aws_secret_access_key,
            tabular_credential,
            S3_BUCKET,
            catalog_name,
            aws_region,
            description='Test PySpark job submit',
        )
        


    s3_path = load_script_s3() 
    submit_glue_job(s3_path)

clean_sneakers_catalog_data()