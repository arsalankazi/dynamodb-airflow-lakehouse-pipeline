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
        
        # FIX 1: Inject event_timestamp into payload for Silver layer deduplication
        # Using ApproximateCreationDateTime from the DynamoDB Stream record (or 'now' as fallback)
        event_ts = record.get("dynamodb", {}).get("ApproximateCreationDateTime", now.timestamp())
        event_timestamp = datetime.fromtimestamp(event_ts, tz=timezone.utc).isoformat()
        item["event_timestamp"] = event_timestamp
        
        # FIX 2: Use eventID for deterministic idempotency (stable across retries)
        event_id = record.get("eventID", "unknown")
        
        # Construct S3 key using Hive-style partitioning
        # Append-only CDC: order_id + event_id ensures each event is a new file, 
        # but retries overwrite the same file.
        key = (
            f"raw/orders/year={now.year}/month={now.month:02d}/"
            f"day={now.day:02d}/hour={now.hour:02d}/"
            f"{item['order_id']}_{event_id}.json"
        )
        
        # Write the item to S3 (Body now includes event_timestamp)
        s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=json.dumps(item))
        written += 1
        
    print(f"Successfully wrote {written} records to s3://{RAW_BUCKET}")
    return {"statusCode": 200, "written": written}