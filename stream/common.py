"""Shared helpers for the stream services: settings, the event filter, SSE parsing and lake paths.

Nothing here talks to Kafka or the network, so the unit tests run without Docker.
"""
import os
from datetime import datetime, timezone

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "wiki.raw")

# kafka-python 2.3 caps one network response at 1 MB, but a fetch across several partitions can return
# up to 1 MB per partition (seen during catch-up as "InvalidReceiveError: Invalid frame length").
# A 30 s session (default 10 s) keeps a consumer in its group through a short stall of the machine.
CONSUMER_SETTINGS = {
    "receive_message_max_bytes": 64 * 1024 * 1024,
    "session_timeout_ms": 30_000,
    "heartbeat_interval_ms": 10_000,
}


def safe_commit(consumer, log) -> bool:
    """Commit offsets; if the commit fails, log it and carry on instead of crashing.

    Any Kafka error is recoverable here: the output is already written, and the next successful commit
    covers these offsets too. At worst, events since the last commit are delivered again after a
    restart (at-least-once; duplicates are removed downstream by compaction and dbt). Seen so far:
    CommitFailedError after a group rebalance (26 Oct, machine stall) and a broker-side UnknownError
    (26 Oct 10:21), each of which used to crash the service."""
    from kafka.errors import KafkaError

    try:
        consumer.commit()
        return True
    except KafkaError as exc:
        log.warning("offset commit failed (%s: %s); will retry with the next commit",
                    exc.__class__.__name__, exc)
        return False

class SendFailed(RuntimeError):
    """A Kafka send failed after the producer's own retries."""


class SendChecker:
    """Remembers whether any Kafka send failed, so a service never commits past a lost event.

    KafkaProducer.send() is asynchronous and flush() does not raise when a send failed, so without this
    a failed send would be lost silently and its input offset or stream position saved anyway. Attach
    `on_error` to every send future; after flush(), call `check()` before committing: it raises
    SendFailed, the service exits, Docker restarts it, and it re-reads from the last commit
    (at-least-once; duplicates are removed downstream)."""

    def __init__(self):
        self.error = None

    def on_error(self, exc):
        if self.error is None:
            self.error = exc

    def watch(self, future):
        future.add_errback(self.on_error)
        return future

    def check(self):
        if self.error is not None:
            raise SendFailed(f"a Kafka send failed, not committing: {self.error!r}")


# Edits and page creations only. Log entries (blocks, moves) and category changes are a different
# kind of activity, and keeping only these two types halves the volume (~20 events/s instead of ~40).
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
