"""Shared helpers for the stream services: settings, the event filter, SSE parsing and lake paths.

Nothing here talks to Kafka or the network, so the unit tests run without Docker.
"""
import os
from datetime import datetime, timezone

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "wiki.raw")

# kafka-python 2.3 caps one network response at 1 MB, but a fetch across several partitions can return
# up to 1 MB per partition (seen during catch-up as "InvalidReceiveError: Invalid frame length").
CONSUMER_SETTINGS = {"receive_message_max_bytes": 64 * 1024 * 1024}

# Edits and page creations only. Log entries (blocks, moves) and category changes are a different
# kind of activity, and keeping only these two types halves the volume (~14 events/s instead of ~29).
KEEP_TYPES = {"edit", "new"}


def keep_event(event: dict) -> bool:
    """True for edits and page creations, False for everything else and Wikimedia's canary test events."""
    if event.get("type") not in KEEP_TYPES:
        return False
    return event.get("meta", {}).get("domain") != "canary"


def event_time(event: dict) -> datetime:
    """When the edit happened (UTC). Uses the edit's own timestamp, falling back to the event metadata."""
    if event.get("timestamp") is not None:
        return datetime.fromtimestamp(int(event["timestamp"]), tz=timezone.utc)
    return datetime.fromisoformat(event["meta"]["dt"].replace("Z", "+00:00"))


def hour_prefix(when: datetime) -> str:
    """Raw-zone prefix for one UTC hour: raw/edits/dt=YYYY-MM-DD/hour=HH (Hive-style partitions)."""
    when = when.astimezone(timezone.utc)
    return f"raw/edits/dt={when:%Y-%m-%d}/hour={when:%H}"


def iter_sse(lines):
    """Yield (event_id, data) for each server-sent event in an iterable of text lines.

    Follows the SSE format: fields are `name: value`, lines starting with ':' are comments, and a
    blank line ends an event. An unfinished event at the end of the input is dropped, as the spec says.
    """
    event_id, data = None, []
    for line in lines:
        if line == "":
            if data:
                yield event_id, "\n".join(data)
            event_id, data = None, []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value
