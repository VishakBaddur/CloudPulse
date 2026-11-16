# One item per event. The conditional write on event_id is what makes
# processing idempotent. Items expire after EVENT_TTL_SECONDS.
resource "aws_dynamodb_table" "events" {
  name         = "${var.project}-events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"

  attribute {
    name = "event_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Per-minute counts by source and level.
# bucket_key = "<minute>#<level>#<shard>", e.g. "2026-11-15T23:31#ERROR#03"
resource "aws_dynamodb_table" "aggregates" {
  name         = "${var.project}-aggregates"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "source"
  range_key    = "bucket_key"

  attribute {
    name = "source"
    type = "S"
  }

  attribute {
    name = "bucket_key"
    type = "S"
  }
}
