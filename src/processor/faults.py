"""Fault injection for resilience testing.

Disabled unless FAULT_INJECTION_ENABLED=true. When enabled, an event can carry
a "fault" field that makes the processor fail in a controlled way:

  transient_error    fails on the first 2 deliveries, then succeeds
  permanent_error    fails on every delivery, so SQS moves it to the DLQ
  crash_after_write  writes the event, then crashes the whole batch once

The other two scenarios need no code here:
  poison_message     a malformed body, rejected by validation
  duplicate_delivery the same event_id sent more than once
"""

import os

from processor.errors import SimulatedCrash, TransientError

TRANSIENT_FAILURES_BEFORE_SUCCESS = 2


def enabled():
    return os.environ.get("FAULT_INJECTION_ENABLED", "false").lower() == "true"


def before_write(event, receive_count):
    if not enabled():
        return
    fault = event.get("fault")
    if fault == "transient_error" and receive_count <= TRANSIENT_FAILURES_BEFORE_SUCCESS:
        raise TransientError(f"injected transient failure (attempt {receive_count})")
    if fault == "permanent_error":
        raise TransientError(f"injected permanent failure (attempt {receive_count})")


def after_write(event, receive_count):
    if not enabled():
        return
    if event.get("fault") == "crash_after_write" and receive_count == 1:
        raise SimulatedCrash("injected crash after write")
