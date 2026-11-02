import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3

# Use LocalStack endpoint for local development
endpoint_url = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
s3 = boto3.client("s3", endpoint_url=endpoint_url)
RAW_BUCKET = os.getenv("RAW_BUCKET", "ecommerce-raw-orders")

def _deserialize(dynamo_item: dict) -> dict:
    """Convert DynamoDB JSON format (e.g., {"S": "value"}) to standard Python dictionary."""
    out = {}
    for key, value in dynamo_item.items():
        if "S" in value:
            out[key] = value["S"]
        elif "N" in value:
            num = Decimal(value["N"])
            out[key] = float(num) if num % 1 else int(num)
        elif "BOOL" in value:
            out[key] = value["BOOL"]
        else:
            out[key] = str(value)
    return out

def lambda_handler(event, context):
    """Triggered by DynamoDB Streams. Writes raw JSON to S3 data lake."""
    now = datetime.now(timezone.utc)
    written = 0
    
    for record in event["Records"]:
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue
        
        # Extract the new image of the item
        item = _deserialize(record["dynamodb"]["NewImage"])
        
        # Construct S3 key using Hive-style partitioning
        key = (
            f"raw/orders/year={now.year}/month={now.month:02d}/"
            f"day={now.day:02d}/hour={now.hour:02d}/{item['order_id']}.json"
        )
        
        # Write the item to S3
        s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=json.dumps(item))
        written += 1
        
    print(f"Successfully wrote {written} records to s3://{RAW_BUCKET}")
    return {"statusCode": 200, "written": written}