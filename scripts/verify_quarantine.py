import boto3
import json
import os
import sys
from decimal import Decimal

# Setup LocalStack connection
ENDPOINT = "http://localhost:4566"
s3 = boto3.client(
    "s3", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test"
)
dynamodb = boto3.resource(
    "dynamodb", endpoint_url=ENDPOINT, region_name="us-east-1",
    aws_access_key_id="test", aws_secret_access_key="test"
)

def print_divider(title):
    print(f"\n{'='*60}\n{title}\n{'='*60}")

def inject_invalid_record():
    """Inserts a deliberately bad record into DynamoDB to test the Quarantine flow."""
    print("[INFO] Injecting a bad record into DynamoDB (quantity=-5, price=-100)...")
    table = dynamodb.Table("Orders")
    table.put_item(Item={
        "order_id": "BAD-ORDER-123",
        "order_date": "2026-11-03",
        "customer_id": "CUST-BAD",
        "product": "Corrupted Item",
        "quantity": -5,
        "price": Decimal("-100.0"),
        "status": "PLACED"
    })
    print("[SUCCESS] Bad record injected successfully!\n")
    print("[INFO] NOW GO TO AIRFLOW UI AND TRIGGER THE DAG.")
    print("[INFO] After the DAG succeeds, run this script again WITHOUT --inject to see the proof.")

def verify_quarantine():
    """Checks the Quarantine bucket and Quality Metrics."""
    print_divider("QUARANTINE BUCKET PROOF (DATA QUALITY GATE)")
    try:
        response = s3.list_objects_v2(Bucket="ecommerce-quarantine", Prefix="orders/")
        if "Contents" not in response:
            print("[INFO] No files found in quarantine. All data is clean!")
        else:
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
            if not files:
                print("[INFO] No JSON records found (only _SUCCESS marker).")
            else:
                print(f"[WARNING] Found {len(files)} quarantined file(s).\n")
                print("Sample Invalid Record:")
                obj = s3.get_object(Bucket="ecommerce-quarantine", Key=files[0])
                bad_data = json.loads(obj['Body'].read().decode('utf-8'))
                print(json.dumps(bad_data, indent=4))
    except Exception as e:
        print(f"[ERROR] Error reading Quarantine: {e}")

    print_divider("DATA QUALITY METRICS (AIRFLOW CIRCUIT BREAKER)")
    try:
        response = s3.list_objects_v2(Bucket="ecommerce-processed-orders", Prefix="_quality/")
        if "Contents" in response:
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
            if files:
                obj = s3.get_object(Bucket="ecommerce-processed-orders", Key=files[0])
                metrics = json.loads(obj['Body'].read().decode('utf-8'))
                print(json.dumps(metrics, indent=4))
                if metrics.get("failure_rate", 0) <= 0.20:
                    print("\n[SUCCESS] Insight: Failure rate is within the 20% threshold.")
                else:
                    print("\n[ERROR] Insight: Failure rate exceeded 20% threshold.")
    except Exception as e:
        print(f"[ERROR] Error reading Quality Metrics: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--inject":
        inject_invalid_record()
    else:
        verify_quarantine()