locals {
  function_name = "${var.project}-processor"
}

data "archive_file" "processor" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/../build/processor.zip"
}

resource "aws_cloudwatch_log_group" "processor" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "processor" {
  name = local.function_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Least privilege: only the actions the processor actually calls.
resource "aws_iam_role_policy" "processor" {
  name = "${local.function_name}-policy"
  role = aws_iam_role.processor.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.processor.arn}:*"
      },
      {
        Sid    = "ConsumeIngestQueue"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ChangeMessageVisibility",
        ]
        Resource = aws_sqs_queue.ingest.arn
      },
      {
        Sid      = "PublishToDlq"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.dlq.arn
      },
      {
        Sid    = "WriteTables"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:ConditionCheckItem",
        ]
        Resource = [
          aws_dynamodb_table.events.arn,
          aws_dynamodb_table.aggregates.arn,
        ]
      },
    ]
  })
}

resource "aws_lambda_function" "processor" {
  function_name    = local.function_name
  role             = aws_iam_role.processor.arn
  runtime          = "python3.13"
  handler          = "processor.handler.handler"
  architectures    = [var.lambda_architecture]
  filename         = data.archive_file.processor.output_path
  source_code_hash = data.archive_file.processor.output_base64sha256
  memory_size      = var.lambda_memory_mb
  timeout          = var.lambda_timeout_seconds

  environment {
    variables = {
      EVENTS_TABLE            = aws_dynamodb_table.events.name
      AGGREGATES_TABLE        = aws_dynamodb_table.aggregates.name
      DLQ_URL                 = aws_sqs_queue.dlq.url
      METRICS_NAMESPACE       = "CloudPulse"
      SERVICE_NAME            = local.function_name
      AGGREGATE_SHARDS        = tostring(var.aggregate_shards)
      BACKOFF_BASE_SECONDS    = tostring(var.backoff_base_seconds)
      BACKOFF_MAX_SECONDS     = tostring(var.backoff_max_seconds)
      FAULT_INJECTION_ENABLED = tostring(var.fault_injection_enabled)
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.processor,
    aws_iam_role_policy.processor,
  ]
}

resource "aws_lambda_event_source_mapping" "ingest" {
  event_source_arn                   = aws_sqs_queue.ingest.arn
  function_name                      = aws_lambda_function.processor.arn
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.batching_window_seconds

  # Only failed records are retried, not the whole batch.
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = var.max_concurrency
  }
}
