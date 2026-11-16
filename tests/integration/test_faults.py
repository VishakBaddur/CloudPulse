"""Fault-injection scenarios.

  1. transient_error     recovers through backoff retries, stored once
  2. permanent_error     exhausts maxReceiveCount and is redriven to the DLQ
  3. poison_message      malformed input goes straight to the DLQ, no retries
  4. duplicate_delivery  an already processed message is delivered again
  5. crash_after_write   the function dies after writing, the batch is redelivered

These need the function deployed with FAULT_INJECTION_ENABLED=true.
"""

import json
import time

import pytest

from tests.integration.conftest import (
    aggregate_count,
    collect_dlq,
    event,
    get_event,
    queue_drained,
    send,
    wait_for_event,
    wait_until,
)

pytestmark = pytest.mark.faults


def _settle(sqs, stack):
    assert wait_until(lambda: queue_drained(sqs, stack["queue_url"]), timeout=180, interval=2)
    time.sleep(2)


def test_transient_error_recovers_with_retries(sqs, ddb, stack, run_id, faults_enabled):
    ev = event(run_id, fault="transient_error")
    sent_at = time.time()
    send(sqs, stack["queue_url"], [ev])
    item = wait_for_event(ddb, stack["events_table"], ev["event_id"], timeout=180)
    assert item, "event never recovered"
    # Two failed attempts with backoff happen before the successful one.
    assert int(item["processed_at"]["N"]) / 1000 - sent_at >= 1
    _settle(sqs, stack)
    assert aggregate_count(ddb, stack["aggregates_table"], ev["source"]) == 1
    assert collect_dlq(sqs, stack["dlq_url"], run_id, timeout=4) == []


def test_permanent_error_is_redriven_to_dlq(sqs, ddb, stack, run_id, faults_enabled):
    ev = event(run_id, fault="permanent_error")
    send(sqs, stack["queue_url"], [ev])
    [msg] = collect_dlq(sqs, stack["dlq_url"], run_id, timeout=240)
    assert json.loads(msg["Body"])["event_id"] == ev["event_id"]
    assert int(msg["Attributes"]["ApproximateReceiveCount"]) >= 1
    assert get_event(ddb, stack["events_table"], ev["event_id"]) is None
    assert aggregate_count(ddb, stack["aggregates_table"], ev["source"]) == 0


def test_poison_message_goes_straight_to_dlq(sqs, ddb, stack, run_id, faults_enabled):
    send(sqs, stack["queue_url"], [f'{{"event_id": "{run_id}-00000000", broken json'])
    started = time.monotonic()
    [msg] = collect_dlq(sqs, stack["dlq_url"], run_id, timeout=120)
    attrs = msg["MessageAttributes"]
    assert attrs["error_type"]["StringValue"] == "validation"
    assert "not valid JSON" in attrs["error_reason"]["StringValue"]
    # Rejected on first delivery, not after a full retry cycle.
    assert time.monotonic() - started < 60
    assert get_event(ddb, stack["events_table"], f"{run_id}-00000000") is None


def test_duplicate_delivery_after_processing(sqs, ddb, stack, run_id, faults_enabled):
    events = [event(run_id, n) for n in range(10)]
    send(sqs, stack["queue_url"], events)
    for ev in events:
        assert wait_for_event(ddb, stack["events_table"], ev["event_id"])
    # Simulate an at-least-once redelivery of the whole batch.
    send(sqs, stack["queue_url"], events)
    send(sqs, stack["queue_url"], events[:5])
    _settle(sqs, stack)
    assert aggregate_count(ddb, stack["aggregates_table"], events[0]["source"]) == 10


def test_crash_after_write_does_not_double_count(sqs, ddb, stack, run_id, faults_enabled):
    events = [event(run_id, n) for n in range(4)]
    events.append(event(run_id, 4, fault="crash_after_write"))
    send(sqs, stack["queue_url"], events)
    for ev in events:
        assert wait_for_event(ddb, stack["events_table"], ev["event_id"], timeout=180)
    _settle(sqs, stack)
    assert aggregate_count(ddb, stack["aggregates_table"], events[0]["source"]) == 5
    assert collect_dlq(sqs, stack["dlq_url"], run_id, timeout=4) == []


def test_mixed_batch_partial_failure(sqs, ddb, stack, run_id, faults_enabled):
    good = event(run_id, 0)
    flaky = event(run_id, 1, fault="transient_error")
    poison = f"not json at all {run_id}"
    send(sqs, stack["queue_url"], [good, flaky, poison])
    assert wait_for_event(ddb, stack["events_table"], good["event_id"])
    assert wait_for_event(ddb, stack["events_table"], flaky["event_id"], timeout=180)
    [msg] = collect_dlq(sqs, stack["dlq_url"], run_id, timeout=60)
    assert msg["Body"] == poison
    _settle(sqs, stack)
    assert aggregate_count(ddb, stack["aggregates_table"], good["source"]) == 2


@pytest.mark.parametrize(
    "mutate, reason",
    [
        (lambda e: e.pop("source"), "missing required fields: source"),
        (lambda e: e.update(level="LOUD"), "level must be one of"),
        (lambda e: e.update(timestamp="yesterday"), "invalid timestamp"),
    ],
    ids=["missing-field", "bad-level", "bad-timestamp"],
)
def test_invalid_events_rejected_with_reason(sqs, ddb, stack, run_id, faults_enabled, mutate, reason):
    ev = event(run_id)
    mutate(ev)
    send(sqs, stack["queue_url"], [ev])
    [msg] = collect_dlq(sqs, stack["dlq_url"], run_id, timeout=120)
    assert reason in msg["MessageAttributes"]["error_reason"]["StringValue"]
    assert get_event(ddb, stack["events_table"], ev["event_id"]) is None
