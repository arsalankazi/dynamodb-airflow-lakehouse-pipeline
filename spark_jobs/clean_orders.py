import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number, to_date, when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
RAW = os.getenv("RAW_BUCKET", "ecommerce-raw-orders")
PROCESSED = os.getenv("PROCESSED_BUCKET", "ecommerce-processed-orders")
QUARANTINE = os.getenv("QUARANTINE_BUCKET", "ecommerce-quarantine")

spark = (
    SparkSession.builder.appName("clean-orders")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_date", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("event_timestamp", StringType(), True),  # Added to support deduplication
])

raw_path = f"s3a://{RAW}/raw/orders/"
try:
    raw_df = spark.read.schema(schema).json(raw_path)
except Exception as e:
    print(f"Schema/read failure: {e}")
    raise

cleaned = (
    raw_df
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("revenue", col("price") * col("quantity"))
    .withColumn("is_cancelled", when(col("status") == "CANCELLED", 1).otherwise(0))
    .withColumn("ingested_at", current_timestamp())
)

# Deduplicate: keep only the latest event for each order_id based on event_timestamp
window_spec = Window.partitionBy("order_id").orderBy(col("event_timestamp").desc())
deduped_df = (
    cleaned
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# Data Quality Checks: separate invalid records (handling nulls)
bad_cond = (
    col("quantity").isNull() | (col("quantity") <= 0) |
    col("price").isNull() | (col("price") <= 0) |
    col("customer_id").isNull()
)

invalid = deduped_df.filter(bad_cond)
valid = deduped_df.filter(~bad_cond)

# Write invalid records to Quarantine bucket (DLQ)
invalid.write.mode("overwrite").json(f"s3a://{QUARANTINE}/orders/")

# Write valid records to Processed bucket in Parquet
(valid.write
    .mode("overwrite")
    .partitionBy("order_date", "status")
    .parquet(f"s3a://{PROCESSED}/orders/"))

# Write quality metrics
total = deduped_df.count()
bad = invalid.count()
summary = {
    "total": total,
    "valid": valid.count(),
    "invalid": bad,
    "failure_rate": round(bad / max(total, 1), 4),
}
spark.createDataFrame([summary]).coalesce(1) \
    .write.mode("overwrite").json(f"s3a://{PROCESSED}/_quality/")

print(f"ETL done: {summary}")
spark.stop()