# Usage: source scripts/aws-env.sh
# Points the AWS CLI, boto3 and Terraform at the real AWS account.
# Run "aws login --profile cloudpulse" first if the session has expired.
unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
export AWS_PROFILE=cloudpulse
export AWS_REGION=us-east-2
export AWS_DEFAULT_REGION=us-east-2
export AWS_PAGER=""
echo "Using real AWS account $(aws sts get-caller-identity --query Account --output text) in $AWS_REGION"
