"""Invoke the deployed function directly to check its response contract."""

import json
import time
import uuid

import pytest

from tests.integration.conftest import collect_dlq, event, get_event


def _record(stack, body, receive_count=1):
    return {
        "messageId": str(uuid.uuid4()),
        "receiptHandle": "test-receipt-handle",
        "body": body if isinstance(body, str) else json.dumps(body),
        "attributes": {
            "ApproximateReceiveCount": str(receive_count),
            "SentTimestamp": str(int(time.time() * 1000)),
        },
        "eventSource": "aws:sqs",
        "eventSourceARN": stack["queue_arn"],
    }


def _invoke(lambda_client, stack, records):
    resp = lambda_client.invoke(
        FunctionName=stack["function_name"],
        Payload=json.dumps({"Records": records}).encode(),
    )
    return resp, json.loads(resp["Payload"].read() or "null")


def test_empty_batch_returns_no_failures(lambda_client, stack):
    resp, body = _invoke(lambda_client, stack, [])
    assert "FunctionError" not in resp
    assert body == {"batchItemFailures": []}


def test_valid_and_duplicate_records_succeed(lambda_client, ddb, stack, run_id):
    ev = event(run_id)
    records = [_record(stack, ev), _record(stack, ev)]
    resp, body = _invoke(lambda_client, stack, records)
    assert "FunctionError" not in resp
    assert body == {"batchItemFailures": []}
    assert get_event(ddb, stack["events_table"], ev["event_id"])


def test_poison_record_is_not_reported_as_failure(lambda_client, sqs, stack, run_id):
    resp, body = _invoke(lambda_client, stack, [_record(stack, f"garbage {run_id}")])
    assert body == {"batchItemFailures": []}
    assert len(collect_dlq(sqs, stack["dlq_url"], run_id, timeout=30)) == 1


def test_only_failed_record_is_reported(lambda_client, stack, run_id, faults_enabled):
    ok = _record(stack, event(run_id, 0))
    bad = _record(stack, event(run_id, 1, fault="permanent_error"))
    resp, body = _invoke(lambda_client, stack, [ok, bad])
    assert body == {"batchItemFailures": [{"itemIdentifier": bad["messageId"]}]}


@pytest.mark.faults
def test_crash_surfaces_as_function_error(lambda_client, ddb, stack, run_id, faults_enabled):
    ev = event(run_id, fault="crash_after_write")
    resp, body = _invoke(lambda_client, stack, [_record(stack, ev)])
    assert resp.get("FunctionError") == "Unhandled"
    assert body["errorType"] == "SimulatedCrash"
    # The write happened before the crash, so the retry will be a duplicate.
    assert get_event(ddb, stack["events_table"], ev["event_id"])
