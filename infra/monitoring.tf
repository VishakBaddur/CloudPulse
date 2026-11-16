# Alarms and the dashboard are skipped on LocalStack, whose CloudWatch
# emulation cannot read alarms back. They are always created on AWS.
locals {
  ns  = "CloudPulse"
  dim = ["Service", local.function_name]
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  count = var.enable_monitoring ? 1 : 0

  alarm_name          = "${var.project}-dlq-not-empty"
  alarm_description   = "Messages have landed in the dead-letter queue"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "ingest_backlog_age" {
  count = var.enable_monitoring ? 1 : 0

  alarm_name          = "${var.project}-ingest-backlog-age"
  alarm_description   = "Oldest message in the ingest queue is older than 5 minutes"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.ingest.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "retry_rate" {
  count = var.enable_monitoring ? 1 : 0

  alarm_name          = "${var.project}-retry-rate"
  alarm_description   = "Sustained retryable processing errors"
  namespace           = local.ns
  metric_name         = "RetryableErrors"
  dimensions          = { Service = local.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 50
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "p99_latency" {
  count = var.enable_monitoring ? 1 : 0

  alarm_name          = "${var.project}-p99-latency"
  alarm_description   = "p99 end-to-end latency above 5 seconds"
  namespace           = local.ns
  metric_name         = "EndToEndLatencyMs"
  dimensions          = { Service = local.function_name }
  extended_statistic  = "p99"
  period              = 300
  evaluation_periods  = 2
  threshold           = 5000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_dashboard" "main" {
  count = var.enable_monitoring ? 1 : 0

  dashboard_name = var.project

  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6
        properties = {
          title  = "Throughput (events/min)"
          region = var.region
          stat   = "Sum"
          period = 60
          metrics = [
            concat([local.ns, "EventsReceived"], local.dim),
            concat([local.ns, "EventsProcessed"], local.dim),
          ]
        }
      },
      {
        type = "metric", x = 12, y = 0, width = 12, height = 6
        properties = {
          title  = "End-to-end latency (ms)"
          region = var.region
          period = 60
          metrics = [
            concat([local.ns, "EndToEndLatencyMs"], local.dim, [{ stat = "p50", label = "p50" }]),
            concat([local.ns, "EndToEndLatencyMs"], local.dim, [{ stat = "p95", label = "p95" }]),
            concat([local.ns, "EndToEndLatencyMs"], local.dim, [{ stat = "p99", label = "p99" }]),
          ]
        }
      },
      {
        type = "metric", x = 0, y = 6, width = 12, height = 6
        properties = {
          title  = "Reliability"
          region = var.region
          stat   = "Sum"
          period = 60
          metrics = [
            concat([local.ns, "DuplicatesSkipped"], local.dim),
            concat([local.ns, "ValidationErrors"], local.dim),
            concat([local.ns, "RetryableErrors"], local.dim),
            concat([local.ns, "ErrorLevelEvents"], local.dim),
          ]
        }
      },
      {
        type = "metric", x = 12, y = 6, width = 12, height = 6
        properties = {
          title  = "Batch duration (ms)"
          region = var.region
          period = 60
          metrics = [
            concat([local.ns, "BatchDurationMs"], local.dim, [{ stat = "p95", label = "p95" }]),
            concat([local.ns, "BatchDurationMs"], local.dim, [{ stat = "p99", label = "p99" }]),
          ]
        }
      },
      {
        type = "metric", x = 0, y = 12, width = 24, height = 6
        properties = {
          title  = "Queues"
          region = var.region
          stat   = "Maximum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ingest.name],
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.dlq.name],
            ["AWS/Lambda", "ConcurrentExecutions", "FunctionName", local.function_name],
          ]
        }
      },
    ]
  })
}
