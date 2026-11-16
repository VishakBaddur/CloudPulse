"""Events flow from SQS through Lambda into DynamoDB correctly."""

import json
from datetime import datetime, timezone

from loadgen import verify
from loadgen.events import make_events
from processor.validation import minute_bucket

from tests.integration.conftest import (
    aggregate_count,
    aggregate_items,
    event,
    send,
    wait_for_event,
)


def test_single_event_is_stored_with_all_fields(sqs, ddb, stack, run_id):
    ev = event(run_id, level="ERROR", message="payment timeout")
    [msg_id] = send(sqs, stack["queue_url"], [ev])
    item = wait_for_event(ddb, stack["events_table"], ev["event_id"])
    assert item, "event was not stored"
    assert item["source"]["S"] == ev["source"]
    assert item["level"]["S"] == "ERROR"
    assert item["message"]["S"] == "payment timeout"
    assert int(item["timestamp"]["N"]) == ev["timestamp"]
    assert item["sqs_message_id"]["S"] == msg_id
    assert int(item["expires_at"]["N"]) > int(item["processed_at"]["N"]) // 1000
    assert int(item["sqs_sent_at"]["N"]) <= int(item["processed_at"]["N"])


def test_aggregate_is_created_for_event(sqs, ddb, stack, run_id):
    ev = event(run_id, level="WARN")
    send(sqs, stack["queue_url"], [ev])
    assert wait_for_event(ddb, stack["events_table"], ev["event_id"])
    [agg] = aggregate_items(ddb, stack["aggregates_table"], ev["source"])
    bucket, level, shard = agg["bucket_key"]["S"].split("#")
    assert bucket == minute_bucket(ev["timestamp"])
    assert level == "WARN"
    assert 0 <= int(shard) < 10
    assert agg["event_count"]["N"] == "1"


def test_batch_of_200_has_no_loss_or_duplicates(sqs, ddb, stack, run_id):
    events = make_events(run_id, 200, seed=7)
    send(sqs, stack["queue_url"], events)
    result = verify.wait_for(
        run_id,
        [e["event_id"] for e in events],
        stack["events_table"],
        stack["aggregates_table"],
        timeout=240,
        ddb=ddb,
    )
    assert result["ok"], result


def test_all_levels_accepted_and_normalized(sqs, ddb, stack, run_id):
    levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL", "error"]
    events = [event(run_id, n, level=lvl) for n, lvl in enumerate(levels)]
    send(sqs, stack["queue_url"], events)
    for n, ev in enumerate(events):
        item = wait_for_event(ddb, stack["events_table"], ev["event_id"])
        assert item, f"level {levels[n]} was not stored"
        assert item["level"]["S"] == levels[n].upper()


def test_iso_and_epoch_timestamps_land_in_same_bucket(sqs, ddb, stack, run_id):
    when = datetime(2026, 11, 15, 23, 31, 42, tzinfo=timezone.utc)
    iso = event(run_id, 0, timestamp=when.isoformat().replace("+00:00", "Z"))
    epoch = event(run_id, 1, timestamp=int(when.timestamp() * 1000))
    send(sqs, stack["queue_url"], [iso, epoch])
    a = wait_for_event(ddb, stack["events_table"], iso["event_id"])
    b = wait_for_event(ddb, stack["events_table"], epoch["event_id"])
    assert a["bucket"]["S"] == b["bucket"]["S"] == "2026-11-15T23:31"
    assert a["timestamp"]["N"] == b["timestamp"]["N"]


def test_attributes_are_persisted(sqs, ddb, stack, run_id):
    attrs = {"region": "us-east-1", "status": 504, "retry": True}
    ev = event(run_id, attributes=attrs)
    send(sqs, stack["queue_url"], [ev])
    item = wait_for_event(ddb, stack["events_table"], ev["event_id"])
    assert json.loads(item["attributes"]["S"]) == attrs


def test_non_ascii_message_round_trips(sqs, ddb, stack, run_id):
    text = "caf\u00e9 \u6771\u4eac \u2713 r\u00e9sum\u00e9"
    ev = event(run_id, message=text)
    send(sqs, stack["queue_url"], [ev])
    item = wait_for_event(ddb, stack["events_table"], ev["event_id"])
    assert item["message"]["S"] == text


def test_aggregate_counts_match_per_service(sqs, ddb, stack, run_id):
    plan = {"auth-service": 7, "search-api": 4}
    events, n = [], 0
    for service, count in plan.items():
        for _ in range(count):
            events.append(event(run_id, n, service=service))
            n += 1
    send(sqs, stack["queue_url"], events)
    for ev in events:
        assert wait_for_event(ddb, stack["events_table"], ev["event_id"])
    for service, count in plan.items():
        assert aggregate_count(ddb, stack["aggregates_table"], f"{run_id}.{service}") == count
