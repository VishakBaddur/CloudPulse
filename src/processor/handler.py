"""CloudPulse log event processor.

Triggered by SQS. For each record it:
  1. validates the event (malformed events go straight to the DLQ)
  2. writes the event and its per-minute aggregate in one DynamoDB
     transaction, guarded by attribute_not_exists(event_id), so a redelivered
     event is detected and skipped (idempotent, no double counting)
  3. reports only the failed records back to SQS (partial batch response)
     and pushes their visibility timeout out with exponential backoff
"""

import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from processor import faults
from processor.errors import SimulatedCrash, TransientError, ValidationError
from processor.metrics import MetricsBatch
from processor.validation import minute_bucket, parse_event

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BOTO_CONFIG = Config(
    retries={"max_attempts": 5, "mode": "adaptive"},
    connect_timeout=2,
    read_timeout=5,
)


@dataclass(frozen=True)
class Settings:
    events_table: str
    aggregates_table: str
    dlq_url: str
    namespace: str
    service: str
    event_ttl_seconds: int
    aggregate_shards: int
    backoff_base_seconds: int
    backoff_max_seconds: int
    tx_max_attempts: int

    @classmethod
    def from_env(cls):
        env = os.environ
        return cls(
            events_table=env["EVENTS_TABLE"],
            aggregates_table=env["AGGREGATES_TABLE"],
            dlq_url=env["DLQ_URL"],
            namespace=env.get("METRICS_NAMESPACE", "CloudPulse"),
            service=env.get("SERVICE_NAME", "cloudpulse-processor"),
            event_ttl_seconds=int(env.get("EVENT_TTL_SECONDS", "604800")),
            aggregate_shards=int(env.get("AGGREGATE_SHARDS", "10")),
            backoff_base_seconds=int(env.get("BACKOFF_BASE_SECONDS", "2")),
            backoff_max_seconds=int(env.get("BACKOFF_MAX_SECONDS", "300")),
            tx_max_attempts=int(env.get("TX_MAX_ATTEMPTS", "4")),
        )


class Clients:
    def __init__(self):
        self.dynamodb = boto3.client("dynamodb", config=BOTO_CONFIG)
        self.sqs = boto3.client("sqs", config=BOTO_CONFIG)
        self.queue_urls = {}


_clients = None


def get_clients():
    global _clients
    if _clients is None:
        _clients = Clients()
    return _clients


def reset_clients():
    """Used by tests to rebuild clients after the environment changes."""
    global _clients
    _clients = None


def _log(level, msg, **fields):
    logger.log(level, json.dumps({"msg": msg, **fields}, default=str))


# ---------------------------------------------------------------- DynamoDB


def aggregate_shard(event_id, shards):
    """Spread aggregate writes over several items to avoid hot-key contention."""
    digest = hashlib.sha1(event_id.encode("utf-8")).hexdigest()
    return int(digest, 16) % shards


def _cancellation_codes(exc):
    reasons = exc.response.get("CancellationReasons") or []
    codes = [r.get("Code") for r in reasons if r]
    if not codes:
        # Some emulators only report reasons inside the message text.
        message = exc.response.get("Error", {}).get("Message", "")
        for code in ("ConditionalCheckFailed", "TransactionConflict"):
            if code in message:
                codes.append(code)
    return codes


def write_event(event, record, clients, settings):
    """Write the event and update its aggregate atomically.

    Returns "processed" for a new event or "duplicate" if this event_id
    was already stored.
    """
    now_ms = int(time.time() * 1000)
    bucket = minute_bucket(event["timestamp"])
    shard = aggregate_shard(event["event_id"], settings.aggregate_shards)
    message_bytes = len(event["message"].encode("utf-8"))

    item = {
        "event_id": {"S": event["event_id"]},
        "source": {"S": event["source"]},
        "level": {"S": event["level"]},
        "message": {"S": event["message"]},
        "timestamp": {"N": str(event["timestamp"])},
        "bucket": {"S": bucket},
        "sqs_message_id": {"S": record["messageId"]},
        "processed_at": {"N": str(now_ms)},
        "expires_at": {"N": str(now_ms // 1000 + settings.event_ttl_seconds)},
    }
    if event["attributes"]:
        item["attributes"] = {"S": json.dumps(event["attributes"], sort_keys=True)}

    transact_items = [
        {
            "Put": {
                "TableName": settings.events_table,
                "Item": item,
                "ConditionExpression": "attribute_not_exists(event_id)",
            }
        },
        {
            "Update": {
                "TableName": settings.aggregates_table,
                "Key": {
                    "source": {"S": event["source"]},
                    "bucket_key": {"S": f"{bucket}#{event['level']}#{shard:02d}"},
                },
                "UpdateExpression": (
                    "ADD event_count :one, message_bytes :bytes "
                    "SET #lvl = :level, #bkt = :bucket, updated_at = :now"
                ),
                "ExpressionAttributeNames": {"#lvl": "level", "#bkt": "bucket"},
                "ExpressionAttributeValues": {
                    ":one": {"N": "1"},
                    ":bytes": {"N": str(message_bytes)},
                    ":level": {"S": event["level"]},
                    ":bucket": {"S": bucket},
                    ":now": {"N": str(now_ms)},
                },
            }
        },
    ]

    for attempt in range(1, settings.tx_max_attempts + 1):
        try:
            clients.dynamodb.transact_write_items(TransactItems=transact_items)
            return "processed"
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "TransactionCanceledException":
                raise
            codes = _cancellation_codes(exc)
            if codes and codes[0] == "ConditionalCheckFailed":
                return "duplicate"
            if "TransactionConflict" in codes and attempt < settings.tx_max_attempts:
                # Full jitter exponential backoff: 0-50ms, 0-100ms, 0-200ms ...
                time.sleep(random.uniform(0, 0.05 * 2 ** (attempt - 1)))
                continue
            raise TransientError(f"transaction cancelled: {codes}") from exc
    raise TransientError("transaction retries exhausted")


# ---------------------------------------------------------------- SQS


def queue_url_from_arn(arn, clients):
    if arn not in clients.queue_urls:
        _, _, _, _, account, name = arn.split(":", 5)
        resp = clients.sqs.get_queue_url(QueueName=name, QueueOwnerAWSAccountId=account)
        clients.queue_urls[arn] = resp["QueueUrl"]
    return clients.queue_urls[arn]


def backoff_seconds(receive_count, base, maximum):
    """Exponential backoff with equal jitter: half fixed, half random."""
    ceiling = min(maximum, base * 2 ** max(receive_count - 1, 0))
    return int(ceiling / 2 + random.uniform(0, ceiling / 2))


def apply_backoff(record, receive_count, clients, settings):
    delay = backoff_seconds(
        receive_count, settings.backoff_base_seconds, settings.backoff_max_seconds
    )
    try:
        url = queue_url_from_arn(record["eventSourceARN"], clients)
        clients.sqs.change_message_visibility(
            QueueUrl=url,
            ReceiptHandle=record["receiptHandle"],
            VisibilityTimeout=delay,
        )
    except ClientError as exc:
        # Best effort: without it the message still retries after the
        # queue's default visibility timeout.
        _log(logging.WARNING, "backoff_failed", error=str(exc))
    return delay


def send_to_dlq(record, reason, clients, settings):
    clients.sqs.send_message(
        QueueUrl=settings.dlq_url,
        MessageBody=record["body"] if record.get("body") else "<empty>",
        MessageAttributes={
            "error_type": {"DataType": "String", "StringValue": "validation"},
            "error_reason": {"DataType": "String", "StringValue": reason[:256] or "unknown"},
            "source_message_id": {"DataType": "String", "StringValue": record["messageId"]},
        },
    )


# ---------------------------------------------------------------- handler


def process_record(record, clients, settings):
    receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
    event = parse_event(record.get("body"))
    faults.before_write(event, receive_count)
    outcome = write_event(event, record, clients, settings)
    faults.after_write(event, receive_count)
    return outcome, event


def handler(event, context):
    settings = Settings.from_env()
    clients = get_clients()
    metrics = MetricsBatch(settings.namespace, settings.service)
    started = time.monotonic()
    records = event.get("Records", [])
    failures = []

    metrics.count("EventsReceived", len(records))

    for record in records:
        message_id = record["messageId"]
        receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
        try:
            outcome, parsed = process_record(record, clients, settings)
        except ValidationError as exc:
            try:
                send_to_dlq(record, str(exc), clients, settings)
                metrics.count("ValidationErrors")
                _log(logging.WARNING, "rejected", message_id=message_id, reason=str(exc))
            except ClientError as dlq_exc:
                metrics.count("RetryableErrors")
                failures.append({"itemIdentifier": message_id})
                _log(logging.ERROR, "dlq_send_failed", message_id=message_id, error=str(dlq_exc))
            continue
        except SimulatedCrash:
            metrics.flush()
            raise
        except Exception as exc:
            metrics.count("RetryableErrors")
            delay = apply_backoff(record, receive_count, clients, settings)
            failures.append({"itemIdentifier": message_id})
            _log(
                logging.WARNING,
                "retry_scheduled",
                message_id=message_id,
                attempt=receive_count,
                backoff_seconds=delay,
                error=str(exc),
            )
            continue

        if outcome == "duplicate":
            metrics.count("DuplicatesSkipped")
            continue

        metrics.count("EventsProcessed")
        if parsed["level"] in ("ERROR", "FATAL"):
            metrics.count("ErrorLevelEvents")
        sent_ts = record.get("attributes", {}).get("SentTimestamp")
        if sent_ts:
            metrics.value("EndToEndLatencyMs", time.time() * 1000 - int(sent_ts))

    metrics.value("BatchDurationMs", (time.monotonic() - started) * 1000)
    metrics.flush()
    return {"batchItemFailures": failures}
