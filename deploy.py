import os
import subprocess
import time
import json
import zipfile
import boto3

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = REGION

print("Starting Infrastructure Deployment...")

# 1. Start Docker
print("Starting Docker containers...")
subprocess.run(["docker", "compose", "up", "-d"], check=True)

# 2. Wait for LocalStack
print("Waiting for LocalStack to be ready...")
s3_client = boto3.client("s3", endpoint_url=ENDPOINT, region_name=REGION)
while True:
    try:
        s3_client.list_buckets()
        break
    except Exception:
        time.sleep(2)
        print(".", end="", flush=True)
print("\nLocalStack is healthy!")

# 3. Create S3 Buckets
print("Creating S3 Buckets...")
buckets = ["ecommerce-raw-orders", "ecommerce-processed-orders", "ecommerce-quarantine", "ecommerce-dashboard"]
for b in buckets:
    try:
        s3_client.create_bucket(Bucket=b)
    except s3_client.exceptions.BucketAlreadyExists:
        pass
print("S3 Buckets ready.")

# 4. Create DynamoDB Table
print("Creating DynamoDB Table & Streams...")
dynamodb_client = boto3.client("dynamodb", endpoint_url=ENDPOINT, region_name=REGION)
try:
    dynamodb_client.create_table(
        TableName="Orders",
        AttributeDefinitions=[
            {"AttributeName": "order_id", "AttributeType": "S"},
            {"AttributeName": "order_date", "AttributeType": "S"},
            {"AttributeName": "customer_id", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "order_id", "KeyType": "HASH"},
            {"AttributeName": "order_date", "KeyType": "RANGE"},
        ],
        # FIX: Added GSI so that 'customer_id' is used in a key schema
        GlobalSecondaryIndexes=[
            {
                "IndexName": "CustomerDateIndex",
                "KeySchema": [
                    {"AttributeName": "customer_id", "KeyType": "HASH"},
                    {"AttributeName": "order_date", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
except dynamodb_client.exceptions.ResourceInUseException:
    pass
print("DynamoDB ready.")

# 5. Deploy Lambda
print("Deploying Lambda Function...")
with zipfile.ZipFile("lambda/function.zip", "w") as z:
    z.write("lambda/dynamodb_stream_to_s3.py", "dynamodb_stream_to_s3.py")

iam_client = boto3.client("iam", endpoint_url=ENDPOINT, region_name=REGION)
lambda_client = boto3.client("lambda", endpoint_url=ENDPOINT, region_name=REGION)

try:
    iam_client.create_role(
        RoleName="lambda-exec-role",
        AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}),
    )
except iam_client.exceptions.EntityAlreadyExistsException:
    pass

with open("lambda/function.zip", "rb") as f:
    zip_bytes = f.read()

try:
    lambda_client.create_function(
        FunctionName="orders-stream-to-s3", Runtime="python3.11", Handler="dynamodb_stream_to_s3.lambda_handler",
        Role="arn:aws:iam::000000000000:role/lambda-exec-role", Code={"ZipFile": zip_bytes},
        Environment={"Variables": {"RAW_BUCKET": "ecommerce-raw-orders", "AWS_ENDPOINT_URL": "http://localstack:4566"}},
        Timeout=30, MemorySize=256,
    )
except lambda_client.exceptions.ResourceConflictException:
    lambda_client.update_function_code(FunctionName="orders-stream-to-s3", ZipFile=zip_bytes)

stream_arn = dynamodb_client.describe_table(TableName="Orders")["Table"]["LatestStreamArn"]
try:
    lambda_client.create_event_source_mapping(EventSourceArn=stream_arn, FunctionName="orders-stream-to-s3", StartingPosition="LATEST")
except lambda_client.exceptions.ResourceConflictException:
    pass

print("\nPIPELINE IS READY!")
print("Airflow: http://localhost:8080")
print("Dashboard: http://localhost:8501")