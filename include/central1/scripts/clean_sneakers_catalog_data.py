import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from typing import Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode, from_json, col, coalesce, to_date, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import boto3

BRANDS = 'brands.parquet'
SNEAKERS = '*/*.parquet'

spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table', "sneakers_catalog", "distinct_sneakers", "s3_bucket"])

# Spark Args
run_date = args['ds']
output_table = args['output_table']
sneakers_catalog = args['sneakers_catalog']
distinct_sneakers = args['distinct_sneakers']
S3_BUCKET = args['s3_bucket']

s3_sneakers_catalog = f"s3://{S3_BUCKET}/{sneakers_catalog}"
s3_distinct_sneakers = f"s3://{S3_BUCKET}/{distinct_sneakers}"

brands = s3_sneakers_catalog + BRANDS
sneakers = s3_sneakers_catalog + SNEAKERS

brand_table = output_table + '_brands'
sneakers_table = output_table + '_sneakers'

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

def clean_brands_raw(spark):
    # Read parquet files from S3 bucket path: brands
    df_brands = spark.read.parquet(brands)
    df_brands = df_brands.select("brands")
    df_brands = df_brands.select(explode("brands").alias("brand"))
    df_brands = df_brands.select("brand.*")
    return df_brands

def clean_sneakers_raw(spark):
    # Read jsons from S3 bucket path: sneakers into a single record

    df_sneakers = spark.read.option("mergeSchema", "true").parquet(sneakers)
    df_sneakers = df_sneakers.select(
        coalesce(col("properties.style_code"), col("properties.stilcode")).alias("id"),
        col("brand"),
        coalesce(col("properties.model"), col("properties.modell")).alias("model"),
        col("properties.gender"),
        coalesce(col("properties.color"), col("properties.farbe")).alias("color"),
        to_date(coalesce(col("properties.date_added"), col("properties.hinzugefügt_am")), "yyyy-MM-dd").alias("date_added"),
        col("properties.sneaker_url"),
        col("properties.sneaker_image"),
        col("error"),
        col("url"),
        col("properties.error").alias("properties_error"),
        col("properties.url").alias("properties_url"),
    )
    return df_sneakers


def get_distinct_brand_models(df):
    # Select brand and model fields and concatenate them with a space between and then get distinct in pyspark with DataFrame API
    df_resp = df.where("error IS NULL AND properties_error IS NULL")
    df_resp = df_resp.select(
        concat(col("brand"), lit(" "), col("model")).alias("brand_model")
    ).distinct()
    return df_resp


df_brands = clean_brands_raw(spark)

df_brands.writeTo(brand_table) \
    .tableProperty("write.spark.fanout.enabled", "true") \
    .createOrReplace()

df_sneakers = clean_sneakers_raw(spark)
df_distinct_brand_models = get_distinct_brand_models(df_sneakers)
# Write df_distinct_brand_models dataframe as parquet in S3

df_distinct_brand_models.write.parquet(s3_distinct_sneakers)

df_sneakers.writeTo(sneakers_table) \
    .tableProperty("write.spark.fanout.enabled", "true") \
    .createOrReplace()
    

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

