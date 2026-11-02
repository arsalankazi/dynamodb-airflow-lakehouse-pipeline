#!/bin/bash
set -e

export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo "1. Packaging Lambda code into a zip file (using Python)..."
# Cross-platform way to zip using Python since Git Bash lacks 'zip'
python -c "import zipfile; z = zipfile.ZipFile('lambda/function.zip', 'w'); z.write('lambda/dynamodb_stream_to_s3.py', 'dynamodb_stream_to_s3.py'); z.close()"

echo "2. Creating IAM Role for Lambda..."
aws --endpoint-url "$AWS_ENDPOINT_URL" iam create-role \
  --role-name lambda-exec-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  2>/dev/null || echo "IAM Role already exists."

echo "3. Deploying Lambda Function..."
aws --endpoint-url "$AWS_ENDPOINT_URL" lambda create-function \
  --function-name orders-stream-to-s3 \
  --runtime python3.11 \
  --handler dynamodb_stream_to_s3.lambda_handler \
  --role arn:aws:iam::000000000000:role/lambda-exec-role \
  --zip-file fileb://lambda/function.zip \
  --environment "Variables={RAW_BUCKET=ecommerce-raw-orders,AWS_ENDPOINT_URL=http://localstack:4566}" \
  --timeout 30 \
  --memory-size 256 \
  2>/dev/null || \
aws --endpoint-url "$AWS_ENDPOINT_URL" lambda update-function-code \
  --function-name orders-stream-to-s3 \
  --zip-file fileb://lambda/function.zip

echo "4. Fetching DynamoDB Stream ARN..."
STREAM_ARN=$(aws --endpoint-url "$AWS_ENDPOINT_URL" dynamodb describe-table \
  --table-name Orders --query 'Table.LatestStreamArn' --output text)

echo "5. Connecting Lambda to DynamoDB Stream..."
aws --endpoint-url "$AWS_ENDPOINT_URL" lambda create-event-source-mapping \
  --function-name orders-stream-to-s3 \
  --event-source-arn "$STREAM_ARN" \
  --starting-position LATEST \
  --batch-size 10 \
  2>/dev/null || echo "Event source mapping already exists."

echo "Lambda deployed and connected to Streams!"