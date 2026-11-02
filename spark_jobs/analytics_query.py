import os

import boto3
from pyspark.sql import SparkSession

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
PROCESSED = os.getenv("PROCESSED_BUCKET", "ecommerce-processed-orders")
DASHBOARD = os.getenv("DASHBOARD_BUCKET", "ecommerce-dashboard")

spark = (
    SparkSession.builder.appName("analytics-query")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# Read processed Parquet data
df = spark.read.parquet(f"s3a://{PROCESSED}/orders/")
df.createOrReplaceTempView("orders")

# Run SQL Query
query = """
    SELECT status, 
           COUNT(order_id) AS order_count, 
           ROUND(SUM(revenue), 2) AS total_revenue 
    FROM orders 
    GROUP BY status 
    ORDER BY total_revenue DESC
"""
result = spark.sql(query).toPandas()

# --- FIX: Use boto3 to upload JSON instead of Pandas to_json ---
s3 = boto3.client(
    "s3", 
    endpoint_url=ENDPOINT, 
    region_name="us-east-1", 
    aws_access_key_id="test", 
    aws_secret_access_key="test"
)

json_data = result.to_json(orient="records")
s3.put_object(Bucket=DASHBOARD, Key="analytics/summary.json", Body=json_data)

print("Analytics query successful. Data written to dashboard bucket.")
spark.stop()