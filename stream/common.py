"""Shared helpers for the stream services: settings, the event filter, SSE parsing and lake paths.

Nothing here talks to Kafka or the network, so the unit tests run without Docker.
"""
import os
from datetime import datetime, timezone
from pathlib import Path

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "wiki.raw")

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


def hour_dir(root: Path, when: datetime) -> Path:
    """Raw-zone folder for one UTC hour: <root>/raw/edits/dt=YYYY-MM-DD/hour=HH (Hive-style partitions)."""
    when = when.astimezone(timezone.utc)
    return Path(root) / "raw" / "edits" / f"dt={when:%Y-%m-%d}" / f"hour={when:%H}"


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
