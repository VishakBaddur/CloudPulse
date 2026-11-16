"""Check that a run was processed with zero loss and zero duplicates.

Loss:        an expected event_id is missing from the events table.
Duplicates:  the aggregate counters add up to more than the number of
             stored events, which would mean an event was counted twice.

Example:
    python -m loadgen.verify --run-id run1a2b3c4d --expected 100000 --wait 600
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

from loadgen.events import event_id

SCAN_SEGMENTS = 8


def dynamodb_client():
    return boto3.client("dynamodb", config=Config(max_pool_connections=SCAN_SEGMENTS * 2))


def _scan_segment(ddb, table, segment, **kwargs):
    items = []
    paginator = ddb.get_paginator("scan")
    for page in paginator.paginate(
        TableName=table, Segment=segment, TotalSegments=SCAN_SEGMENTS, **kwargs
    ):
        items.extend(page["Items"])
    return items


def parallel_scan(ddb, table, **kwargs):
    with ThreadPoolExecutor(max_workers=SCAN_SEGMENTS) as pool:
        parts = pool.map(lambda s: _scan_segment(ddb, table, s, **kwargs), range(SCAN_SEGMENTS))
    return [item for part in parts for item in part]


def stored_event_ids(ddb, table, run_id):
    items = parallel_scan(
        ddb,
        table,
        FilterExpression="begins_with(event_id, :p)",
        ExpressionAttributeValues={":p": {"S": f"{run_id}-"}},
        ProjectionExpression="event_id",
    )
    return [i["event_id"]["S"] for i in items]


def aggregate_total(ddb, table, run_id):
    items = parallel_scan(
        ddb,
        table,
        FilterExpression="begins_with(#src, :p)",
        ExpressionAttributeNames={"#src": "source"},
        ExpressionAttributeValues={":p": {"S": f"{run_id}."}},
        ProjectionExpression="event_count",
    )
    return sum(int(i["event_count"]["N"]) for i in items)


def check(run_id, expected_ids, events_table, aggregates_table, ddb=None):
    ddb = ddb or dynamodb_client()
    expected = set(expected_ids)
    stored = stored_event_ids(ddb, events_table, run_id)
    stored_set = set(stored)
    agg = aggregate_total(ddb, aggregates_table, run_id)
    missing = expected - stored_set
    unexpected = stored_set - expected
    return {
        "run_id": run_id,
        "expected": len(expected),
        "stored": len(stored_set),
        "missing": len(missing),
        "unexpected": len(unexpected),
        "aggregate_total": agg,
        "double_counted": agg - len(stored_set),
        "ok": not missing and not unexpected and agg == len(stored_set),
        "missing_sample": sorted(missing)[:5],
    }


def wait_for(run_id, expected_ids, events_table, aggregates_table, timeout=300, interval=3, ddb=None):
    """Poll until every expected event is stored, or the timeout passes."""
    ddb = ddb or dynamodb_client()
    deadline = time.monotonic() + timeout
    while True:
        result = check(run_id, expected_ids, events_table, aggregates_table, ddb)
        if result["missing"] == 0 or time.monotonic() >= deadline:
            return result
        time.sleep(interval)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected", type=int, required=True, help="number of events sent")
    parser.add_argument("--events-table", default="cloudpulse-events")
    parser.add_argument("--aggregates-table", default="cloudpulse-aggregates")
    parser.add_argument("--wait", type=int, default=0, help="seconds to wait for processing")
    args = parser.parse_args(argv)

    ids = [event_id(args.run_id, i) for i in range(args.expected)]
    started = time.monotonic()
    result = wait_for(args.run_id, ids, args.events_table, args.aggregates_table, timeout=args.wait)
    result["verify_seconds"] = round(time.monotonic() - started, 1)
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
