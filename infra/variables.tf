variable "region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Name prefix for every resource"
  type        = string
  default     = "cloudpulse"
}

variable "lambda_architecture" {
  description = "arm64 is cheaper on AWS; use x86_64 on x86 CI runners"
  type        = string
  default     = "arm64"

  validation {
    condition     = contains(["arm64", "x86_64"], var.lambda_architecture)
    error_message = "lambda_architecture must be arm64 or x86_64."
  }
}

variable "lambda_memory_mb" {
  type    = number
  default = 256
}

variable "lambda_timeout_seconds" {
  type    = number
  default = 30
}

variable "batch_size" {
  description = "Max SQS messages per Lambda invocation"
  type        = number
  default     = 10
}

variable "batching_window_seconds" {
  description = "How long the poller waits to fill a batch (required >= 1 when batch_size > 10)"
  type        = number
  default     = 0
}

variable "max_concurrency" {
  description = "Upper bound on concurrent Lambda invocations from the queue (min 2)"
  type        = number
  default     = 10
}

variable "max_receive_count" {
  description = "Deliveries before SQS moves a message to the DLQ"
  type        = number
  default     = 5
}

variable "backoff_base_seconds" {
  type    = number
  default = 2
}

variable "backoff_max_seconds" {
  type    = number
  default = 300
}

variable "aggregate_shards" {
  description = "Write shards per aggregate key to avoid hot partitions"
  type        = number
  default     = 10
}

variable "fault_injection_enabled" {
  description = "Allow events to trigger injected faults. Keep false outside of testing."
  type        = bool
  default     = false
}

variable "log_retention_days" {
  type    = number
  default = 7
}

variable "enable_monitoring" {
  description = "Create CloudWatch alarms and the dashboard (disabled on LocalStack)"
  type        = bool
  default     = true
}
