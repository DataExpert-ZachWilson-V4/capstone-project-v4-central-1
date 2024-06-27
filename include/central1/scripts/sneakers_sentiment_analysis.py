import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.types import  FloatType
from pyspark.sql.functions import udf, col
from textblob import TextBlob

spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'input_data_path', "output_table", "s3_bucket"])

# Spark Args
run_date = args['ds']
input_data_path = args['input_data_path']
output_table = args['output_table']
S3_BUCKET = args['s3_bucket']

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

def get_sentiment(text):
    sentiment = TextBlob(text).sentiment.polarity
    return sentiment

sentiment_score_udf = udf(lambda x: get_sentiment(x), FloatType())

def sentiment_analysis(df_tweets, input_table: str, output_table: str):
    df_tweets = df_tweets.withColumn("tweet_sentiment", sentiment_score_udf(col("text")).alias("sentiment_score"))
    return df_tweets

def get_tweets(spark, input_data_path: str):
    full_s3_path = f"s3://{S3_BUCKET}/{input_data_path}"
    return spark.read.option("mergeSchema", "true").parquet(full_s3_path)

df = get_tweets(spark, input_data_path)
df = sentiment_analysis(df, input_data_path, output_table)
df.writeTo(output_table) \
    .tableProperty("write.spark.fanout.enabled", "true") \
    .createOrReplace()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
