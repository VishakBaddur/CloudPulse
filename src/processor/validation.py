"""Parsing and validation of incoming log events."""

import json
from datetime import datetime, timezone

from processor.errors import ValidationError

REQUIRED_FIELDS = ("event_id", "source", "level", "message", "timestamp")
LEVELS = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
MAX_EVENT_ID_LEN = 128
MAX_SOURCE_LEN = 64
MAX_MESSAGE_BYTES = 8192


def _parse_timestamp(value):
    """Accept epoch milliseconds or an ISO 8601 string. Return epoch milliseconds."""
    if isinstance(value, bool):
        raise ValidationError("timestamp must be a number or ISO 8601 string")
    if isinstance(value, (int, float)):
        if value <= 0:
            raise ValidationError("timestamp must be positive")
        return int(value)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValidationError(f"invalid timestamp: {value!r}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    raise ValidationError("timestamp must be a number or ISO 8601 string")


def _require_str(data, field, max_len):
    value = data[field]
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} must be a non-empty string")
    if len(value) > max_len:
        raise ValidationError(f"{field} exceeds {max_len} characters")
    return value


def parse_event(body):
    """Parse an SQS message body into a normalized event dict.

    Raises ValidationError for anything that can never be processed.
    """
    try:
        data = json.loads(body)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"body is not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ValidationError("event must be a JSON object")

    missing = [f for f in REQUIRED_FIELDS if f not in data]
    if missing:
        raise ValidationError(f"missing required fields: {', '.join(missing)}")

    event_id = _require_str(data, "event_id", MAX_EVENT_ID_LEN)
    source = _require_str(data, "source", MAX_SOURCE_LEN)

    level = data["level"]
    if not isinstance(level, str) or level.upper() not in LEVELS:
        raise ValidationError(f"level must be one of {sorted(LEVELS)}")

    message = data["message"]
    if not isinstance(message, str):
        raise ValidationError("message must be a string")
    if len(message.encode("utf-8")) > MAX_MESSAGE_BYTES:
        raise ValidationError(f"message exceeds {MAX_MESSAGE_BYTES} bytes")

    attributes = data.get("attributes", {})
    if not isinstance(attributes, dict):
        raise ValidationError("attributes must be an object")

    event = {
        "event_id": event_id,
        "source": source,
        "level": level.upper(),
        "message": message,
        "timestamp": _parse_timestamp(data["timestamp"]),
        "attributes": attributes,
    }
    fault = data.get("fault")
    if isinstance(fault, str):
        event["fault"] = fault
    return event


def minute_bucket(epoch_ms):
    """Return the UTC minute an event belongs to, e.g. 2026-11-15T23:31."""
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M")
