"""Throughput and latency benchmarks against the deployed CloudPulse stack.

Two kinds of scenario:

  burst   send N events as fast as possible and measure how quickly the
          pipeline drains them at a given Lambda concurrency (throughput)
  steady  send events at a fixed rate below capacity and measure
          end-to-end latency from SQS send to DynamoDB write

Latency is computed exactly for every event from two timestamps written by
AWS: SentTimestamp (when SQS accepted the message) and processed_at (when
the Lambda committed the write). CloudWatch percentiles from the
EndToEndLatencyMs custom metric are fetched as a cross-check.

Example:
    python -m benchmarks.run_benchmark --burst-size 20000 --concurrency 2 5 10 --rates 100 250
"""

import argparse
import json
import math
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

from loadgen.events import make_events
from loadgen.producer import queue_url, send_events, send_paced, sqs_client
from loadgen.verify import dynamodb_client, parallel_scan

PROJECT = "cloudpulse"
FUNCTION = f"{PROJECT}-processor"
QUEUE = f"{PROJECT}-ingest"
EVENTS_TABLE = f"{PROJECT}-events"
RESULTS_DIR = Path(__file__).parent / "results"


def percentile(sorted_values, p):
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_values:
        return None
    rank = math.ceil(p / 100 * len(sorted_values))
    return sorted_values[max(0, min(len(sorted_values) - 1, rank - 1))]


def esm_uuid(lam, queue_arn):
    mappings = lam.list_event_source_mappings(FunctionName=FUNCTION, EventSourceArn=queue_arn)
    return mappings["EventSourceMappings"][0]["UUID"]


def set_concurrency(lam, mapping_uuid, value):
    lam.update_event_source_mapping(UUID=mapping_uuid, ScalingConfig={"MaximumConcurrency": value})
    for _ in range(60):
        cfg = lam.get_event_source_mapping(UUID=mapping_uuid)
        if cfg["State"] == "Enabled" and cfg["ScalingConfig"]["MaximumConcurrency"] == value:
            return
        time.sleep(2)
    raise RuntimeError("event source mapping did not settle")


def wait_drained(sqs, url, timeout=1800):
    """Wait until the queue reports empty twice in a row."""
    deadline = time.monotonic() + timeout
    zero_reads = 0
    while time.monotonic() < deadline:
        attrs = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        waiting = int(attrs["ApproximateNumberOfMessages"])
        in_flight = int(attrs["ApproximateNumberOfMessagesNotVisible"])
        zero_reads = zero_reads + 1 if waiting == 0 and in_flight == 0 else 0
        if zero_reads >= 2:
            return
        print(f"    {datetime.now():%H:%M:%S} waiting={waiting} in_flight={in_flight}", flush=True)
        time.sleep(5)
    raise TimeoutError("queue did not drain")


def collect(ddb, run_id):
    items = parallel_scan(
        ddb,
        EVENTS_TABLE,
        FilterExpression="begins_with(event_id, :p)",
        ExpressionAttributeValues={":p": {"S": f"{run_id}-"}},
        ProjectionExpression="processed_at, sqs_sent_at",
    )
    return [
        (int(i["sqs_sent_at"]["N"]), int(i["processed_at"]["N"]))
        for i in items
        if "sqs_sent_at" in i
    ]


def summarize(name, run_id, expected, pairs, **extra):
    latencies = sorted(done - sent for sent, done in pairs)
    first_sent = min(sent for sent, _ in pairs)
    last_done = max(done for _, done in pairs)
    duration = (last_done - first_sent) / 1000
    return {
        "scenario": name,
        "run_id": run_id,
        "expected": expected,
        "processed": len(pairs),
        "duration_s": round(duration, 1),
        "throughput_eps": round(len(pairs) / duration, 1),
        "latency_ms": {
            "p50": percentile(latencies, 50),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": latencies[-1],
        },
        "window": [first_sent, last_done],
        **extra,
    }


def cloudwatch_latency(cw, window):
    """p50/p95/p99 of the EndToEndLatencyMs custom metric over a window."""
    start = datetime.fromtimestamp(window[0] / 1000, tz=timezone.utc).replace(second=0, microsecond=0)
    end = datetime.fromtimestamp(window[1] / 1000, tz=timezone.utc) + timedelta(minutes=1)
    period = max(60, math.ceil((end - start).total_seconds() / 60) * 60)
    resp = cw.get_metric_statistics(
        Namespace="CloudPulse",
        MetricName="EndToEndLatencyMs",
        Dimensions=[{"Name": "Service", "Value": FUNCTION}],
        StartTime=start,
        EndTime=start + timedelta(seconds=period),
        Period=period,
        ExtendedStatistics=["p50", "p95", "p99"],
        Unit="Milliseconds",
    )
    points = resp.get("Datapoints", [])
    if not points:
        return None
    return {k: round(v) for k, v in points[0]["ExtendedStatistics"].items()}


def run_burst(ctx, size, concurrency):
    run_id = f"bb{concurrency}x{uuid.uuid4().hex[:6]}"
    print(f"\n[burst] {size} events, max concurrency {concurrency} ({run_id})", flush=True)
    set_concurrency(ctx["lam"], ctx["esm"], concurrency)
    events = make_events(run_id, size)
    sent, send_s = send_events(events, ctx["url"], concurrency=32, sqs=ctx["sqs"])
    wait_drained(ctx["sqs"], ctx["url"])
    time.sleep(3)
    pairs = collect(ctx["ddb"], run_id)
    return summarize(
        f"burst-c{concurrency}", run_id, size, pairs,
        lambda_concurrency=concurrency, send_rate_eps=round(sent / send_s, 1),
    )


def run_steady(ctx, rate, seconds, concurrency):
    run_id = f"bs{rate}x{uuid.uuid4().hex[:6]}"
    count = rate * seconds
    print(f"\n[steady] {rate} events/s for {seconds}s, max concurrency {concurrency} ({run_id})", flush=True)
    set_concurrency(ctx["lam"], ctx["esm"], concurrency)
    events = make_events(run_id, count)
    sent, send_s = send_paced(events, ctx["url"], rate, concurrency=16, sqs=ctx["sqs"])
    wait_drained(ctx["sqs"], ctx["url"])
    time.sleep(3)
    pairs = collect(ctx["ddb"], run_id)
    return summarize(
        f"steady-{rate}eps", run_id, count, pairs,
        lambda_concurrency=concurrency, target_rate_eps=rate, actual_send_rate_eps=round(sent / send_s, 1),
    )


def markdown_table(results):
    lines = [
        "| Scenario | Lambda concurrency | Events | Throughput (events/s) | p50 (ms) | p95 (ms) | p99 (ms) |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in results:
        lat = r["latency_ms"]
        lines.append(
            f"| {r['scenario']} | {r['lambda_concurrency']} | {r['processed']:,} | "
            f"{r['throughput_eps']:,} | {lat['p50']:,} | {lat['p95']:,} | {lat['p99']:,} |"
        )
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--burst-size", type=int, default=20000)
    parser.add_argument("--concurrency", type=int, nargs="*", default=[2, 5, 10])
    parser.add_argument("--rates", type=int, nargs="*", default=[100, 250])
    parser.add_argument("--steady-seconds", type=int, default=60)
    parser.add_argument("--restore-concurrency", type=int, default=10)
    parser.add_argument("--cloudwatch-wait", type=int, default=120,
                        help="seconds to wait for EMF metrics before reading CloudWatch")
    args = parser.parse_args(argv)

    sqs = sqs_client(32)
    url = queue_url(sqs, QUEUE)
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    lam = boto3.client("lambda")
    ctx = {"sqs": sqs, "url": url, "lam": lam, "esm": esm_uuid(lam, arn), "ddb": dynamodb_client()}

    results = []
    try:
        for c in args.concurrency:
            results.append(run_burst(ctx, args.burst_size, c))
        for rate in args.rates:
            results.append(run_steady(ctx, rate, args.steady_seconds, args.restore_concurrency))
    finally:
        set_concurrency(lam, ctx["esm"], args.restore_concurrency)

    if args.cloudwatch_wait:
        print(f"\nwaiting {args.cloudwatch_wait}s for CloudWatch to ingest EMF metrics...", flush=True)
        time.sleep(args.cloudwatch_wait)
        cw = boto3.client("cloudwatch")
        for r in results:
            r["cloudwatch_latency_ms"] = cloudwatch_latency(cw, r["window"])

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS_DIR / f"benchmark-{stamp}.json"
    out.write_text(json.dumps({"generated_at": stamp, "results": results}, indent=2))
    table = markdown_table(results)
    (RESULTS_DIR / f"benchmark-{stamp}.md").write_text(table + "\n")

    print("\n" + table)
    print(f"\nSaved {out}")


if __name__ == "__main__":
    main()
