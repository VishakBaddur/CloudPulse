"""Synthetic log event generation."""

import random
import time

SERVICES = (
    "auth-service",
    "checkout-api",
    "inventory-service",
    "payments-gateway",
    "search-api",
    "notification-worker",
    "user-profile",
    "recommendation-engine",
)

LEVEL_WEIGHTS = (
    ("DEBUG", 6),
    ("INFO", 70),
    ("WARN", 15),
    ("ERROR", 8),
    ("FATAL", 1),
)

MESSAGES = {
    "DEBUG": ("cache lookup key={key}", "request headers parsed in {ms}ms"),
    "INFO": ("request completed status=200 in {ms}ms", "user {user} signed in", "order {key} created"),
    "WARN": ("slow query took {ms}ms", "retrying upstream call attempt={n}"),
    "ERROR": ("upstream timeout after {ms}ms", "payment declined for order {key}"),
    "FATAL": ("out of memory, worker restarting",),
}

_LEVELS = [lvl for lvl, _ in LEVEL_WEIGHTS]
_WEIGHTS = [w for _, w in LEVEL_WEIGHTS]


def event_id(run_id, seq):
    return f"{run_id}-{seq:08d}"


def source_name(run_id, service):
    """Sources are prefixed with the run id so each run's aggregates can be verified alone."""
    return f"{run_id}.{service}"


def make_event(run_id, seq, rng=None, fault=None):
    rng = rng or random
    level = rng.choices(_LEVELS, weights=_WEIGHTS, k=1)[0]
    template = rng.choice(MESSAGES[level])
    message = template.format(
        ms=rng.randint(1, 5000),
        key=rng.randint(100000, 999999),
        user=f"u{rng.randint(1, 50000)}",
        n=rng.randint(1, 5),
    )
    event = {
        "event_id": event_id(run_id, seq),
        "source": source_name(run_id, rng.choice(SERVICES)),
        "level": level,
        "message": message,
        "timestamp": int(time.time() * 1000),
        "attributes": {"run_id": run_id, "seq": seq},
    }
    if fault:
        event["fault"] = fault
    return event


def make_events(run_id, count, start=0, seed=None):
    rng = random.Random(seed)
    return [make_event(run_id, seq, rng) for seq in range(start, start + count)]
