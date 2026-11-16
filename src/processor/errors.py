"""Error types used by the processor to decide how a failed record is handled."""


class ValidationError(Exception):
    """The event is malformed. Retrying will never help, so it goes straight to the DLQ."""


class TransientError(Exception):
    """A temporary failure. The record is returned to SQS and retried with backoff."""


class SimulatedCrash(Exception):
    """Raised only by fault injection to simulate the function dying mid-batch."""
