# Simple DAG with TaskFlow API
import os
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.central1.utils.glue_job_submission import create_glue_job
from include.central1.utils.twitter.scraper import scrape_tweets
import polars as pl

airflow_home=os.environ['AIRFLOW_HOME']

PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_sneakers_sa'
PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/activate'
PATH_TO_DBT_VARS = f'{airflow_home}/dbt_sneakers_sa/dbt.env'

ENTRYPOINT_CMD = f"source {PATH_TO_DBT_VENV} && source {PATH_TO_DBT_VARS}"
S3_BUCKET = Variable.get("AWS_S3_BUCKET_TABULAR")
N_SNEAKERS = 20

root_s3_script_key = "central1/scripts/sneakers_sentiment_analysis.py"
root_s3_distinct_sneakers_key = "central1/distinct_sneakers/"
root_s3_tweets_key = "central1/tweets_data/"


def dbt_run_command(command) -> str:
    """Function that returns the dbt command to be executed.

    Args:
        command (str or list[str]): Single command or list of commands to be executed.

    Returns:
        str: Final DBT command to be executed.
    """
    base_command = f"{ENTRYPOINT_CMD} && dbt"
    if isinstance(command, list):
        return f"{base_command} {' && dbt '.join(command)}" 
        # Join multiple dbt commands with ' && dbt'
    return f"{base_command} {command}"

@dag(
    # Schedule to run each hour at minute 30
    schedule_interval='30 * * * *',
    # schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
    catchup=False)
def sneakers_sentiment_analysis():
    """Submits Glue Job to perform sentiment analysis on sneakers tweets"""

    @task()
    def get_disctinct_sneakers():
        s3_hook = S3Hook()

        # Download files from S3 and read them with polars.
        # The files are stored in the root_s3_distinct_sneakers_key folder.
        # List all .parquet files in the folder and download them to the include folder.
        s3_path = f"s3://{S3_BUCKET}/{root_s3_distinct_sneakers_key}"
        objects_list = s3_hook.list_keys(S3_BUCKET,root_s3_distinct_sneakers_key)
        
        if not objects_list:
            logging.warning("No files found in %s", s3_path)
            return []
        # Download the files
        all_dir_path = []
        for object_key in objects_list:
            local_path = os.path.join("include", object_key)
            all_dir_path.append(local_path)
            s3_hook.download_file(object_key, S3_BUCKET, local_path, preserve_file_name=True)
            logging.info("Downloaded %s to %s", object_key, local_path)

        all_files_path = []
        for dir_path in all_dir_path:
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    all_files_path.append(os.path.join(root, file))
        df = pl.read_parquet(all_files_path)
        # Get Variable "CATALOG_CHECKPOINT" and filter the data: CATALOG_CHECKPOINT if not exists create the Variable it and set it to 0

        try:
            catalog_checkpoint_info = Variable.get("CATALOG_CHECKPOINT_INFO", deserialize_json=True)
            catalog_checkpoint =  catalog_checkpoint_info["checkpoint"]
        except TypeError:
            logging.info("Variable CATALOG_CHECKPOINT_INFO not found, creating it")
            catalog_checkpoint = 0
            Variable.set("CATALOG_CHECKPOINT_INFO", { "checkpoint": catalog_checkpoint , "total_sneakers" : df.shape[0] }, serialize_json=True)

        next_catalog_checkpoint = catalog_checkpoint + N_SNEAKERS

        return df["brand_model"].to_list()[catalog_checkpoint:next_catalog_checkpoint]

    #Airflow task to that extracts twitter data given backfilling date from Airflow metadata arg
    @task()
    def extract_tweets(*args, **kwargs):
        # Get dag execution start_time
        
        dag_run = kwargs.get("dag_run")
        dag_start_date = dag_run.start_date
        sneakers =  args[0]
        logging.info("%d sneakers will be processed for %s datetime", len(sneakers), str(dag_start_date))
        extracted_tweets = scrape_tweets(sneakers, S3_BUCKET, dag_start_date)
        if not extracted_tweets:
            raise ValueError(f"No tweets extracted for datetime {str(dag_start_date)}")
        return root_s3_tweets_key
        
    @task()
    def load_script_s3():
        s3_hook = S3Hook()

        local_script_path = os.path.join("include", root_s3_script_key)
        s3_hook.load_file(
            local_script_path,
            root_s3_script_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
        s3_path_script = f"s3://{S3_BUCKET}/{root_s3_script_key}"
        logging.info("Script %s successfully uploaded to %s", local_script_path, s3_path_script)
        return s3_path_script

    @task()
    def submit_glue_job(*args, **kwargs):
        # Get dag run start date, and at it to root_s3_tweets_key separated by year/month/day
        dag_run = kwargs.get("dag_run")
        dag_start_date = dag_run.start_date
        s3_tweets = os.path.join(root_s3_tweets_key, dag_start_date.strftime("%Y/%m/%d"), "tweets.parquet")
        s3_script = args[0]

        job_name = "incremental_sneakers_sentiment_analysis"
        script_path= s3_script
        aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
        aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
        tabular_credential = Variable.get("TABULAR_CREDENTIAL")
        catalog_name = Variable.get("CATALOG_NAME")
        aws_region = Variable.get("AWS_GLUE_REGION")
        description= "Testing Job Spark"
        arguments = {
                "--ds": "{{ ds }}",
                "--input_data_path": s3_tweets,
                "--output_table": "jsgomez14.incremental_sneakers_twitter_sentiment_analysis",
                "--s3_bucket": S3_BUCKET,
            }
        python_modules = "textblob==0.18.0.post0"
        

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
            additional_python_modules= python_modules,
        )
    # Run task if upstream tasks failed too

    @task(trigger_rule="all_done",)
    def set_next_catalog_checkpoint():
        catalog_checkpoint_info = Variable.get("CATALOG_CHECKPOINT_INFO", deserialize_json=True)
        catalog_checkpoint =  catalog_checkpoint_info["checkpoint"]
        total_sneakers =  catalog_checkpoint_info["total_sneakers"]

        next_catalog_checkpoint = catalog_checkpoint + N_SNEAKERS
        if next_catalog_checkpoint >= total_sneakers:
            next_catalog_checkpoint = 0

        Variable.set("CATALOG_CHECKPOINT_INFO", { "checkpoint": next_catalog_checkpoint , "total_sneakers" : total_sneakers }, serialize_json=True)
    
    dbt_run_stg_models = task.bash(
            dbt_run_command, # Function that returns the dbt bash command to be executed
            task_id = "dbt_run_stg_models",
            cwd = PATH_TO_DBT_PROJECT,
        )(["deps","run -s sneakers_sa.staging.*"])

    dbt_run_marts_models = task.bash(
            dbt_run_command, # Function that returns the dbt bash command to be executed
            task_id = "dbt_run_marts_models",
            cwd = PATH_TO_DBT_PROJECT,
        )(["deps","run -s sneakers_sa.marts.*"])

    dbt_run_snapshots = task.bash(
            dbt_run_command, # Function that returns the dbt bash command to be executed
            task_id = "dbt_run_snapshots",
            cwd = PATH_TO_DBT_PROJECT,
        )(["deps","snapshot -s sneakers_sa.*"])

    s3_script = load_script_s3()

    extract_tweets(get_disctinct_sneakers()) >>  submit_glue_job(s3_script) \
        >> [set_next_catalog_checkpoint(),dbt_run_stg_models] >> dbt_run_marts_models \
            >> dbt_run_snapshots



sneakers_sentiment_analysis()