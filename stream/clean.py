"""Stream processing logic: validate and clean one raw event, and count edits per minute.

No Kafka or network here, so all of it is unit-tested (tests/test_stream_clean.py).
The cleaned event drops the user name and the edit summary (they can hold personal information);
only a hash of the user name (to count distinct editors) and the editor type are kept.
"""
import hashlib
import re
from collections import defaultdict

IPV4 = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
IPV6 = re.compile(r"^[0-9A-Fa-f:]+:[0-9A-Fa-f:]+$")
REVERT = re.compile(r"undid revision|\bundo\b|\brevert|\brv\b|rollback")


class InvalidEvent(ValueError):
    """Raised for events that can't be cleaned; the message is the reason sent to the dead-letter topic."""


def editor_type(user: str, is_bot: bool) -> str:
    """bot, anonymous (temporary account or IP address: logged-out editors) or registered."""
    if is_bot:
        return "bot"
    if user.startswith("~") or IPV4.match(user) or IPV6.match(user):
        return "anonymous"
    return "registered"


def is_revert(comment) -> bool:
    """Heuristic: edit summaries that undo or revert someone else's change."""
    return bool(REVERT.search((comment or "").lower()))


def clean_event(event: dict) -> dict:
    """Validated, typed, privacy-safe version of one raw recentchange event."""
    meta = event.get("meta") or {}
    if not meta.get("id"):
        raise InvalidEvent("missing meta.id")
    if event.get("type") not in ("edit", "new"):
        raise InvalidEvent(f"unexpected type {event.get('type')!r}")
    for field in ("wiki", "title", "user"):
        if not isinstance(event.get(field), str) or not event[field]:
            raise InvalidEvent(f"missing {field}")
    if not isinstance(event.get("timestamp"), int):
        raise InvalidEvent("missing or non-integer timestamp")
    if not isinstance(event.get("bot"), bool):
        raise InvalidEvent("missing bot flag")

    length = event.get("length") or {}
    revision = event.get("revision") or {}
    return {
        "event_id": meta["id"],
        "recent_change_id": event.get("id"),
        "revision_id": revision.get("new"),
        "event_ts": event["timestamp"],
        "wiki_id": event["wiki"],
        "server_name": event.get("server_name"),
        "namespace": event.get("namespace"),
        "title": event["title"],
        "is_new_page": event["type"] == "new",
        "is_bot": event["bot"],
        "is_minor": bool(event.get("minor")),
        "editor_type": editor_type(event["user"], event["bot"]),
        "editor_key": hashlib.md5(event["user"].encode()).hexdigest(),
        "bytes_changed": (length.get("new") or 0) - (length.get("old") or 0),
        "is_revert": is_revert(event.get("comment")),
    }


class MinuteCounter:
    """Tumbling one-minute windows of edits per wiki, split bot / human, in event time.

    Time is tracked per source (Kafka partition), and the watermark is the *slowest* source's newest
    event: when the processor catches up on a backlog, partitions are read unevenly, and one racing
    ahead must not make the others' events look late. A minute is emitted once the watermark is
    `lateness_s` past that minute's end, so slightly out-of-order events are still counted. Events for
    a minute already emitted are counted in `late_events` and dropped (the warehouse, not this live
    view, is the source of truth).
    """

    def __init__(self, lateness_s: int = 120):
        self.lateness_s = lateness_s
        self.counts = defaultdict(lambda: [0, 0])  # (minute_ts, wiki) -> [bot_edits, human_edits]
        self.newest_by_source = {}
        self.emitted_before = 0  # every minute < this has been emitted
        self.late_events = 0

    def add(self, event_ts: int, wiki: str, is_bot: bool, source=0):
        minute = event_ts - event_ts % 60
        if minute < self.emitted_before:
            self.late_events += 1
            return
        self.counts[(minute, wiki)][0 if is_bot else 1] += 1
        self.newest_by_source[source] = max(self.newest_by_source.get(source, 0), event_ts)

    def watermark(self, sources=None) -> int:
        """Newest event time that every source has reached. A source not seen yet holds it at 0."""
        sources = sources if sources is not None else self.newest_by_source.keys()
        return min((self.newest_by_source.get(s, 0) for s in sources), default=0)

    def pop_closed(self, sources=None) -> list:
        """Rows for every minute whose window has closed: (minute_ts, wiki, bot_edits, human_edits).

        `sources`: the partitions currently assigned (so one that has sent nothing yet holds windows open).
        """
        # minute m is closed when m + 60 + lateness <= watermark, i.e. m < floor_to_minute(watermark - lateness)
        cutoff = self.watermark(sources) - self.lateness_s
        if cutoff <= 0:
            return []
        cutoff -= cutoff % 60
        closed = sorted(key for key in self.counts if key[0] < cutoff)
        rows = [(minute, wiki, *self.counts.pop((minute, wiki))) for minute, wiki in closed]
        self.emitted_before = max(self.emitted_before, cutoff)
        return rows
