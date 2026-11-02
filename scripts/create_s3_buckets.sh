#!/bin/bash
set -e
export AWS_ENDPOINT_URL=http://localhost:4566

for bucket in ecommerce-raw-orders ecommerce-processed-orders ecommerce-quarantine ecommerce-dashboard; do
  aws --endpoint-url "$AWS_ENDPOINT_URL" s3 mb "s3://$bucket" 2>/dev/null || echo "bucket $bucket exists"
done
echo "S3 buckets ready"