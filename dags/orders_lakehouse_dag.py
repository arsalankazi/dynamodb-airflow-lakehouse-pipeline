import json
import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
RAW_BUCKET = os.getenv("RAW_BUCKET", "ecommerce-raw-orders")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "ecommerce-processed-orders")
FAILURE_THRESHOLD = 0.20

default_args = {
    "owner": "Arsalan_Kazi",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def _s3():
    return boto3.client("s3", endpoint_url=ENDPOINT, region_name=REGION)

def quality_gate(**_):
    s3 = _s3()
    objs = s3.list_objects_v2(Bucket=PROCESSED_BUCKET, Prefix="_quality/").get("Contents", [])
    if not objs:
        raise AirflowFailException("No quality metrics found")
    
    # FIX: Filter out the _SUCCESS file and only look for .json files
    json_objs = [obj for obj in objs if obj["Key"].endswith(".json")]
    if not json_objs:
        raise AirflowFailException("No valid JSON quality metrics found")
    
    # Get the latest JSON file
    latest = max(json_objs, key=lambda o: o["LastModified"])
    body = s3.get_object(Bucket=PROCESSED_BUCKET, Key=latest["Key"])["Body"].read()
    
    # Parse the JSON
    metrics = json.loads(body)
    
    print(f"Quality metrics: {metrics}")
    if metrics["failure_rate"] > FAILURE_THRESHOLD:
        raise AirflowFailException(f"Failure rate {metrics['failure_rate']} > {FAILURE_THRESHOLD}")
    return metrics

with DAG(
    dag_id="orders_lakehouse_etl",
    schedule=None, # Triggered manually for now
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dynamodb", "pyspark", "athena", "lakehouse"],
) as dag:

    seed = BashOperator(
        task_id="seed_dynamodb",
        # Explicitly set LocalStack endpoint for Docker network resolution
        bash_command="AWS_ENDPOINT_URL=http://localstack:4566 python /opt/airflow/scripts/seed_orders.py",
    )

    wait_raw = S3KeySensor(
        task_id="wait_for_lambda_s3",
        bucket_name=RAW_BUCKET,
        bucket_key="raw/orders/year=*/month=*/day=*/*.json",  
        wildcard_match=True,
        aws_conn_id="localstack",
        timeout=120,
        poke_interval=10,
        mode="poke",
    )

    spark_clean = BashOperator(
        task_id="spark_clean_validate",
        bash_command=(
            "spark-submit --master local[*] "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/airflow/spark_jobs/clean_orders.py"
        ),
    )

    gate = PythonOperator(task_id="quality_gate", python_callable=quality_gate)

    # NEW TASK: Run the Analytics Query to feed the Dashboard
    analytics = BashOperator(
        task_id="run_analytics_dashboard",
        bash_command=(
            "spark-submit --master local[*] "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/airflow/spark_jobs/analytics_query.py"
        ),
    )

    # Updated dependency chain: gate -> analytics
    seed >> wait_raw >> spark_clean >> gate >> analytics