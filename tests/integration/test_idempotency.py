"""Redelivered or repeated events are stored and counted exactly once."""

import time
from concurrent.futures import ThreadPoolExecutor

from loadgen import verify

from tests.integration.conftest import (
    aggregate_count,
    event,
    get_event,
    queue_drained,
    send,
    wait_for_event,
    wait_until,
)


def _settle(sqs, stack):
    """Wait until the queue is empty so every copy has been processed."""
    assert wait_until(lambda: queue_drained(sqs, stack["queue_url"]), timeout=180, interval=2)
    time.sleep(2)


def test_same_event_sent_five_times_is_stored_once(sqs, ddb, stack, run_id):
    ev = event(run_id)
    for _ in range(5):
        send(sqs, stack["queue_url"], [ev])
    assert wait_for_event(ddb, stack["events_table"], ev["event_id"])
    _settle(sqs, stack)
    assert aggregate_count(ddb, stack["aggregates_table"], ev["source"]) == 1


def test_duplicates_inside_one_batch(sqs, ddb, stack, run_id):
    ev = event(run_id)
    send(sqs, stack["queue_url"], [ev, ev, ev])
    assert wait_for_event(ddb, stack["events_table"], ev["event_id"])
    _settle(sqs, stack)
    assert aggregate_count(ddb, stack["aggregates_table"], ev["source"]) == 1


def test_first_write_wins_for_conflicting_payloads(sqs, ddb, stack, run_id):
    first = event(run_id, message="original")
    send(sqs, stack["queue_url"], [first])
    assert wait_for_event(ddb, stack["events_table"], first["event_id"])
    second = dict(first, message="tampered", level="FATAL")
    send(sqs, stack["queue_url"], [second])
    _settle(sqs, stack)
    item = get_event(ddb, stack["events_table"], first["event_id"])
    assert item["message"]["S"] == "original"
    assert item["level"]["S"] == "INFO"


def test_concurrent_double_send_of_100_events(sqs, ddb, stack, run_id):
    events = [event(run_id, n, service=f"svc{n % 4}") for n in range(100)]
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: send(sqs, stack["queue_url"], events), range(2)))
    result = verify.wait_for(
        run_id,
        [e["event_id"] for e in events],
        stack["events_table"],
        stack["aggregates_table"],
        timeout=240,
        ddb=ddb,
    )
    assert result["missing"] == 0, result
    _settle(sqs, stack)
    result = verify.check(
        run_id, [e["event_id"] for e in events], stack["events_table"], stack["aggregates_table"], ddb
    )
    assert result["ok"], result
    assert result["double_counted"] == 0
