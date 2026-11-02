import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

# FIX: Read from environment variable, default to localhost for local runs
ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT,
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
table = dynamodb.Table("Orders")

PRODUCTS = ["Laptop", "Phone", "Headphones", "Keyboard", "Monitor"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]

def seed(count=50):
    now = datetime.now(timezone.utc)
    for _ in range(count):
        order_date = (now - timedelta(days=random.randint(0, 3))).strftime("%Y-%m-%d")
        table.put_item(Item={
            "order_id": str(uuid.uuid4()),
            "order_date": order_date,
            "customer_id": f"CUST-{random.randint(100, 110)}",
            "product": random.choice(PRODUCTS),
            "quantity": random.randint(1, 5),
            "price": Decimal(str(round(random.uniform(100, 1500), 2))), 
            "status": random.choice(STATUSES),
        })
    print(f"Seeded {count} orders successfully!")

if __name__ == "__main__":
    seed(5000) 