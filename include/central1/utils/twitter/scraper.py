import os
from ntscraper import Nitter
import logging
import polars as pl
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from time import sleep

def clean_tweets_df(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if col in ('quoted-post', 'pictures', 'videos', 'gifs', 'replying-to', 'external-link'):
            df = df.drop(col)

    df = df.unnest("user")
    df = df.unnest("stats")
    return df

def load_current_tweets(tweets_output, dag_start_date, suffix):
    # Load tweets_output to polars dataframe then upload to S3
    df = pl.DataFrame(tweets_output)
    df = clean_tweets_df(df)
    # Save to parquet

    start_date = dag_start_date.strftime("%Y/%m/%d/")
    start_hour = dag_start_date.strftime("%H")

    local_dir = f"central1/tweets_data/"
    tweets_path = os.path.join("include", local_dir, start_date)
    # Create directory if not exists:
    if not os.path.exists(os.path.dirname(tweets_path)):
        os.makedirs(os.path.dirname(tweets_path))
    local_file_parquet = os.path.join(tweets_path, f"tweets_hour_{start_hour}_{suffix}.parquet")
    df.write_parquet(local_file_parquet)

def get_tweets_output(nitter, sneaker):
    logging.info(f"Processing {sneaker}")
    for i in range(5):
        curr_instance = "https://nitter.privacydev.net" if i == 0 else nitter.get_random_instance()
        output = nitter.get_tweets(sneaker, mode='term', number=20, instance=curr_instance)
        if output.get("tweets", None):
            break
        sleep(5)
    return output

def consolidate_parquets(s3_bucket):
    data = os.path.join("include", "central1", "tweets_data")
    files = [os.path.join(data, f) for f in os.listdir(data) if f.endswith('.parquet')]
    if not files:
        logging.warning("No files to consolidated.")
        return
    df = pl.read_parquet(files)

    for col in df.columns:
        if col in ('quoted-post', 'pictures', 'videos', 'gifs', 'replying-to', 'external-link'):
            df = df.drop(col)

    df = df.unnest("user")
    df = df.unnest("stats")

    consolidated_file_path = f"{data}tweets.parquet"
    df.write_parquet(consolidated_file_path)

    #Load consolidated file to S3
    s3_hook = S3Hook()
    s3_key = consolidated_file_path.replace("include/", "")
    s3_hook.load_file(
        consolidated_file_path,
        s3_key,
        bucket_name=s3_bucket,
        replace=True,
    )
    s3_path = f"s3://{s3_bucket}/{s3_key}"
    logging.info("File %s successfully uploaded to %s", consolidated_file_path, s3_path)
    # Clean up local files after consolidation
    for f in files:
        os.remove(f)
    os.remove(consolidated_file_path)
    

def scrape_tweets(sneakers, s3_bucket, dag_start_date):
    nitter = Nitter()

    tweets_output = []
    # We have 63552 sneakers
    counter = 1
    total_tweets = 0
    steps = 5
    suffix = 0
    for sneaker in sneakers:
        logging.info("Progress: %s/%s - Percentage: %s", counter, len(sneakers), int(counter/len(sneakers)*100))
        tweets_collector = []
        suffix = int(counter/steps)
        output = get_tweets_output(nitter, sneaker)
        
        tweets = output["tweets"]

        if tweets:
            for tweet in tweets:
                model = {"model" : sneaker }
                model.update(tweet)
                tweets_collector.append(model)
        # Append model to each tweet at the start
        logging.info("%s Gathered %d tweets.", sneaker, len(tweets_collector))
        tweets_output.extend(tweets_collector)

        if counter % steps == 0:
            total_tweets += len(tweets_output)
            if tweets_output:
                load_current_tweets(tweets_output, dag_start_date, suffix)
                tweets_output = []
        
        counter += 1
        sleep(5)
    # Load the remaining tweets if any
    if tweets_output:
        total_tweets += len(tweets_output)
        load_current_tweets(tweets_output, dag_start_date, suffix)
    logging.info("Done. Total Tweets Loaded: %d", total_tweets)
    consolidate_parquets(s3_bucket)
    return total_tweets > 0

