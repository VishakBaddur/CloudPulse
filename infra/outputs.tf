output "ingest_queue_url" {
  value = aws_sqs_queue.ingest.url
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.url
}

output "events_table" {
  value = aws_dynamodb_table.events.name
}

output "aggregates_table" {
  value = aws_dynamodb_table.aggregates.name
}

output "function_name" {
  value = aws_lambda_function.processor.function_name
}

output "dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${aws_cloudwatch_dashboard.main.dashboard_name}"
}
