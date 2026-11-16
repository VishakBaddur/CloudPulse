# Usage: source scripts/local-env.sh
# Points the AWS CLI and boto3 at LocalStack for this terminal session only.
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566
echo "AWS endpoint set to LocalStack ($AWS_ENDPOINT_URL)"
export AWS_PAGER=""
