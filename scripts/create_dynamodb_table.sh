#!/bin/bash
set -e
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws --endpoint-url "$AWS_ENDPOINT_URL" dynamodb create-table \
  --table-name Orders \
  --attribute-definitions \
    AttributeName=order_id,AttributeType=S \
    AttributeName=order_date,AttributeType=S \
    AttributeName=customer_id,AttributeType=S \
  --key-schema \
    AttributeName=order_id,KeyType=HASH \
    AttributeName=order_date,KeyType=RANGE \
  --global-secondary-indexes '[{
    "IndexName":"CustomerDateIndex",
    "KeySchema":[
      {"AttributeName":"customer_id","KeyType":"HASH"},
      {"AttributeName":"order_date","KeyType":"RANGE"}
    ],
    "Projection":{"ProjectionType":"ALL"}
  }]' \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

echo " DynamoDB Orders table + Streams ready"