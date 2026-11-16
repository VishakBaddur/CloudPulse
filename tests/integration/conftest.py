"""Fixtures for tests that run against the deployed CloudPulse stack.

By default the tests target LocalStack. Set AWS_ENDPOINT_URL to an empty
string and use real credentials to run them against AWS instead.
"""

import json
import os
import time
import uuid

import boto3
import pytest
from botocore.config import Config

PROJECT = os.environ.get("CLOUDPULSE_PROJECT", "cloudpulse")
QUEUE_NAME = f"{PROJECT}-ingest"
DLQ_NAME = f"{PROJECT}-dlq"
EVENTS_TABLE = f"{PROJECT}-events"
AGGREGATES_TABLE = f"{PROJECT}-aggregates"
FUNCTION_NAME = f"{PROJECT}-processor"

DEFAULT_TIMEOUT = 120


def _setdefault_env():
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    if "AWS_ENDPOINT_URL" not in os.environ:
        os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    if os.environ["AWS_ENDPOINT_URL"] == "":
        del os.environ["AWS_ENDPOINT_URL"]


_setdefault_env()

_config = Config(max_pool_connections=32, retries={"max_attempts": 5, "mode": "standard"})


def pytest_collection_modifyitems(items):
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def sqs():
    return boto3.client("sqs", config=_config)


@pytest.fixture(scope="session")
def ddb():
    return boto3.client("dynamodb", config=_config)


@pytest.fixture(scope="session")
def lambda_client():
    return boto3.client("lambda", config=_config)


@pytest.fixture(scope="session")
def stack(sqs, lambda_client):
    """Resolve resource identifiers and skip everything if the stack is not deployed."""
    try:
        queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        dlq_url = sqs.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"]
        fn = lambda_client.get_function_configuration(FunctionName=FUNCTION_NAME)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"CloudPulse stack not reachable: {exc}")
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sqs.purge_queue(QueueUrl=dlq_url)
    return {
        "queue_url": queue_url,
        "queue_arn": queue_arn,
        "dlq_url": dlq_url,
        "events_table": EVENTS_TABLE,
        "aggregates_table": AGGREGATES_TABLE,
        "function_name": FUNCTION_NAME,
        "function_env": fn.get("Environment", {}).get("Variables", {}),
        "function_config": fn,
    }


@pytest.fixture
def run_id():
    return f"it{uuid.uuid4().hex[:10]}"


@pytest.fixture
def faults_enabled(stack):
    if stack["function_env"].get("FAULT_INJECTION_ENABLED") != "true":
        pytest.skip("fault injection is disabled on the deployed function")


# ---------------------------------------------------------------- helpers


def wait_until(fn, timeout=DEFAULT_TIMEOUT, interval=1.0):
    """Call fn until it returns a truthy value. Returns the last value."""
    deadline = time.monotonic() + timeout
    while True:
        value = fn()
        if value or time.monotonic() >= deadline:
            return value
        time.sleep(interval)


def send(sqs, queue_url, events):
    """Send events (dicts or raw strings) in batches of 10. Returns message ids."""
    bodies = [e if isinstance(e, str) else json.dumps(e) for e in events]
    ids = []
    for i in range(0, len(bodies), 10):
        chunk = bodies[i : i + 10]
        resp = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[{"Id": str(n), "MessageBody": b} for n, b in enumerate(chunk)],
        )
        assert not resp.get("Failed"), resp.get("Failed")
        ids.extend(m["MessageId"] for m in resp["Successful"])
    return ids


def get_event(ddb, table, event_id):
    resp = ddb.get_item(
        TableName=table, Key={"event_id": {"S": event_id}}, ConsistentRead=True
    )
    return resp.get("Item")


def wait_for_event(ddb, table, event_id, timeout=DEFAULT_TIMEOUT):
    return wait_until(lambda: get_event(ddb, table, event_id), timeout=timeout)


def aggregate_items(ddb, table, source):
    resp = ddb.query(
        TableName=table,
        KeyConditionExpression="#s = :s",
        ExpressionAttributeNames={"#s": "source"},
        ExpressionAttributeValues={":s": {"S": source}},
        ConsistentRead=True,
    )
    return resp["Items"]


def aggregate_count(ddb, table, source):
    return sum(int(i["event_count"]["N"]) for i in aggregate_items(ddb, table, source))


def collect_dlq(sqs, dlq_url, needle, timeout=DEFAULT_TIMEOUT, expected=1):
    """Receive DLQ messages whose body contains needle, deleting them as they are found."""
    found = []
    deadline = time.monotonic() + timeout
    while len(found) < expected and time.monotonic() < deadline:
        resp = sqs.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            VisibilityTimeout=2,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )
        for msg in resp.get("Messages", []):
            if needle in msg["Body"]:
                found.append(msg)
                sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=msg["ReceiptHandle"])
    return found


def queue_drained(sqs, queue_url):
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"]) == 0 and int(
        attrs["ApproximateNumberOfMessagesNotVisible"]
    ) == 0


def event(run_id, n=0, service="checkout-api", level="INFO", **extra):
    body = {
        "event_id": f"{run_id}-{n:08d}",
        "source": f"{run_id}.{service}",
        "level": level,
        "message": f"test message {n}",
        "timestamp": int(time.time() * 1000),
    }
    body.update(extra)
    return body
