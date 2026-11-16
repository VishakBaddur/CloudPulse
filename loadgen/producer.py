"""Send log events to the ingest queue in parallel batches of 10.

Example:
    python -m loadgen.producer --count 100000 --concurrency 16
"""

import argparse
import json
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

from loadgen.events import make_events

SQS_BATCH_LIMIT = 10
MAX_SEND_ATTEMPTS = 6


def sqs_client(concurrency=16):
    return boto3.client(
        "sqs",
        config=Config(
            max_pool_connections=max(10, concurrency),
            retries={"max_attempts": 5, "mode": "adaptive"},
        ),
    )


def queue_url(sqs, name):
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]


def _chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def send_batch(sqs, url, bodies):
    """Send up to 10 bodies. Entries that fail are retried with backoff.

    Returns the number of messages SQS accepted.
    """
    pending = {str(i): body for i, body in enumerate(bodies)}
    for attempt in range(1, MAX_SEND_ATTEMPTS + 1):
        entries = [{"Id": k, "MessageBody": v} for k, v in pending.items()]
        resp = sqs.send_message_batch(QueueUrl=url, Entries=entries)
        for ok in resp.get("Successful", []):
            pending.pop(ok["Id"], None)
        if not pending:
            return len(bodies)
        retryable = [f for f in resp.get("Failed", []) if not f.get("SenderFault")]
        if len(retryable) < len(resp.get("Failed", [])):
            raise RuntimeError(f"SQS rejected messages: {resp['Failed']}")
        time.sleep(min(2.0, 0.05 * 2**attempt) * random.random())
    raise RuntimeError(f"{len(pending)} messages not accepted after {MAX_SEND_ATTEMPTS} attempts")


def send_bodies(bodies, url, concurrency=16, sqs=None):
    """Send raw message bodies concurrently. Returns (sent, seconds)."""
    sqs = sqs or sqs_client(concurrency)
    started = time.perf_counter()
    batches = list(_chunks(bodies, SQS_BATCH_LIMIT))
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        sent = sum(pool.map(lambda b: send_batch(sqs, url, b), batches))
    return sent, time.perf_counter() - started


def send_events(events, url, concurrency=16, sqs=None):
    return send_bodies([json.dumps(e) for e in events], url, concurrency, sqs)


def send_paced(events, url, rate, concurrency=16, sqs=None):
    """Send events at a steady rate (events per second). Returns (sent, seconds)."""
    sqs = sqs or sqs_client(concurrency)
    batches = list(_chunks([json.dumps(e) for e in events], SQS_BATCH_LIMIT))
    interval = SQS_BATCH_LIMIT / rate
    started = time.perf_counter()
    futures = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for i, batch in enumerate(batches):
            delay = started + i * interval - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            futures.append(pool.submit(send_batch, sqs, url, batch))
        sent = sum(f.result() for f in futures)
    return sent, time.perf_counter() - started


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--queue-name", default="cloudpulse-ingest")
    parser.add_argument("--run-id", default=None, help="defaults to a random id")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)

    run_id = args.run_id or f"run{uuid.uuid4().hex[:8]}"
    sqs = sqs_client(args.concurrency)
    url = queue_url(sqs, args.queue_name)

    events = make_events(run_id, args.count, seed=args.seed)
    sent, seconds = send_events(events, url, args.concurrency, sqs)
    print(json.dumps({
        "run_id": run_id,
        "sent": sent,
        "seconds": round(seconds, 2),
        "send_rate_per_sec": round(sent / seconds, 1) if seconds else None,
    }))
    return run_id


if __name__ == "__main__":
    main()
