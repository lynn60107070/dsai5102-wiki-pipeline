"""Producer: Wikimedia EventStreams (recentchange) -> Kafka topic `wiki.raw`.

Keeps only edits and page creations and keys each event by wiki (so one wiki always lands in the same
partition, in order). Two connections, so a backfill can never hold back live data:

  live      always connects from *now* and remembers the time of the last event it forwarded. Any jump
            of more than 3 s between consecutive events (laptop asleep, offline, a dropped connection)
            and, after a restart, everything since the saved time, is queued as a gap (from, until)
  backfill  a background thread replays each gap with `?since=<from>` and stops once the replay is
            past `until`. Gaps are persisted, so a restart mid-backfill carries on.

Lessons from 27-28 Oct, all handled here:
  - Resuming with a `Last-Event-ID` header was silently ignored by Wikimedia (the stream started from
    live), so backfills "finished" with almost nothing. `?since=<time>` works, and is fast.
  - Wikimedia's hourly "canary" test events carry arbitrary timestamps (a replay starts with some
    stamped hours ahead); they are ignored by all time logic and never stored.
  - A connection that survives the laptop sleeping can serve old events slowly: if 100 events in a row
    are more than 3 minutes old, reconnect from now (the jump is then queued).
  - (3 Nov) Wikimedia drops connections every few minutes, so a long replay saves how far it got and a
    dropped replay resumes there instead of starting the gap over.
Overlaps between live and backfill only create duplicates, which compaction and dbt remove.

Run inside Docker: `python -m stream.producer`
"""
import json
import logging
import os
import signal
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from stream.common import BOOTSTRAP, RAW_TOPIC, iter_sse, keep_event

STREAM_URL = os.environ.get("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
USER_AGENT = os.environ.get("WIKI_USER_AGENT", "")  # Wikimedia's API policy requires a descriptive one
STATE_DIR = Path(os.environ.get("STATE_DIR", "/state"))
POSITION_FILE = STATE_DIR / "last_event_time"
LEGACY_ID_FILE = STATE_DIR / "last_event_id"     # before 28 Oct the position was saved as an event id
GAPS_FILE = STATE_DIR / "pending_gaps.json"
SAVE_EVERY_S = 10
LOG_EVERY_S = 60
LIVE_MAX_LAG = timedelta(minutes=3)

log = logging.getLogger("producer")
_gaps_lock = threading.Lock()


# --- small pieces, unit-tested in tests/test_stream_common.py -----------------------------------------

def is_canary(event: dict) -> bool:
    """Wikimedia's hourly test events: arbitrary timestamps, never stored, never used for time logic."""
    return event.get("meta", {}).get("domain") == "canary"


def arrival_time(event: dict) -> datetime:
    """When Wikimedia published the event (meta.dt): the stream's own order."""
    return datetime.fromisoformat(event["meta"]["dt"].replace("Z", "+00:00"))


def id_time(event_id: str):
    """The time inside an EventStreams id (`[{"topic": ..., "timestamp": <ms>}, ...]`), or None."""
    try:
        stamps = [p["timestamp"] for p in json.loads(event_id) if p.get("timestamp")]
    except (ValueError, TypeError, KeyError):
        return None
    return datetime.fromtimestamp(max(stamps) / 1000, tz=timezone.utc) if stamps else None


def since_param(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Streak:
    """True once a condition has held for `needed` events in a row (single odd events never decide)."""

    def __init__(self, needed: int):
        self.needed, self.run = needed, 0

    def update(self, condition: bool) -> bool:
        self.run = self.run + 1 if condition else 0
        return self.run >= self.needed


class GapEnd:
    """Has the replay passed the end of the gap? After 200 events in a row at or after `until`."""

    def __init__(self, until: datetime, needed: int = 200):
        self.until, self.streak = until, Streak(needed)

    def passed(self, when: datetime) -> bool:
        return self.streak.update(when >= self.until)


class ReplayProgress:
    """How far a replay has safely got: the oldest of the last `window` event times. A stray event from
    later in the gap moves the newest time but never this one, so saving it can never skip events."""

    def __init__(self, window: int = 500):
        self.recent = deque(maxlen=window)

    def add(self, when: datetime):
        self.recent.append(when)

    def safe_point(self):
        return min(self.recent) if len(self.recent) == self.recent.maxlen else None


class StreamContinuity:
    """Detects a jump in the live stream: consecutive real events are normally milliseconds apart, so a
    jump of more than `max_jump` means events were missed. Old stray events never move the clock back."""

    def __init__(self, max_jump: timedelta = timedelta(seconds=3)):
        self.max_jump, self.last_time = max_jump, None

    def jumped(self, when: datetime) -> bool:
        jump = self.last_time is not None and when - self.last_time > self.max_jump
        if self.last_time is None or when > self.last_time:
            self.last_time = when
        return jump


class LiveFellBehind(Exception):
    """The live connection is serving old events (e.g. it survived the laptop sleeping): reconnect."""


# --- state: the last position and the queue of gaps (persisted, so restarts lose nothing) -------------

def _write_atomic(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text)
    tmp.replace(path)


def load_position():
    if POSITION_FILE.exists():
        return datetime.fromisoformat(POSITION_FILE.read_text().strip())
    if LEGACY_ID_FILE.exists():
        return id_time(LEGACY_ID_FILE.read_text().strip())
    return None


def save_position(when):
    if when:
        _write_atomic(POSITION_FILE, when.isoformat())


def _read_gaps() -> list:
    gaps = json.loads(GAPS_FILE.read_text()) if GAPS_FILE.exists() else []
    for gap in gaps:  # gaps queued before 28 Oct stored an event id instead of a time
        if "since" not in gap and id_time(gap.get("from_id", "")):
            gap["since"] = id_time(gap["from_id"]).isoformat()
    return [g for g in gaps if "since" in g]


def load_gaps() -> list:
    with _gaps_lock:
        return _read_gaps()


def add_gap(since: datetime, until: datetime):
    with _gaps_lock:
        gaps = _read_gaps()
        gaps.append({"since": since.isoformat(), "until": until.isoformat()})
        _write_atomic(GAPS_FILE, json.dumps(gaps))


def advance_gap(since: datetime, index: int = 0):
    """Move a gap's start forward to where its replay has safely got (never backwards)."""
    with _gaps_lock:
        gaps = _read_gaps()
        if index < len(gaps) and since > datetime.fromisoformat(gaps[index]["since"]):
            gaps[index]["since"] = since.isoformat()
            _write_atomic(GAPS_FILE, json.dumps(gaps))


def finish_gap(index: int = 0):
    with _gaps_lock:
        gaps = _read_gaps()
        if index < len(gaps):
            gaps.pop(index)
            _write_atomic(GAPS_FILE, json.dumps(gaps))


# --- the two connections ----------------------------------------------------------------------------

def connect_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", compression_type="gzip",
                                 linger_ms=200, retries=5)
        except NoBrokersAvailable:
            log.info("Kafka not reachable at %s yet, retrying in 5 s", BOOTSTRAP)
            time.sleep(5)


def read_stream(since=None):
    params = {"since": since_param(since)} if since else None
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    with requests.get(STREAM_URL, params=params, headers=headers, stream=True, timeout=(10, 60)) as response:
        response.raise_for_status()
        response.encoding = "utf-8"
        yield from iter_sse(response.iter_lines(decode_unicode=True))


def send(producer: KafkaProducer, event: dict, data: str):
    producer.send(RAW_TOPIC, key=event.get("wiki", "unknown").encode(), value=data.encode())


def save_replay_progress(producer: KafkaProducer, progress: ReplayProgress):
    """Remember how far the current replay got, so a dropped connection resumes there (3 Nov: a 16 h gap
    takes ~12 min to replay, Wikimedia drops connections every few minutes, and restarting from the
    beginning each time looped for 2 hours, re-sending the same events)."""
    point = progress.safe_point()
    if point:
        producer.flush()  # only remember positions of events that are safely in Kafka
        advance_gap(point)


def backfill_worker(producer: KafkaProducer):
    """Replay queued gaps one at a time, oldest first, at whatever speed Wikimedia serves them."""
    while True:
        gaps = load_gaps()
        if not gaps:
            time.sleep(10)
            continue
        since = datetime.fromisoformat(gaps[0]["since"])
        until = datetime.fromisoformat(gaps[0]["until"])
        log.info("backfill: replaying %s → %s UTC (%d gap(s) queued)",
                 f"{since:%Y-%m-%d %H:%M:%S}", f"{until:%H:%M:%S}", len(gaps))
        sent, last_log, frontier, end = 0, time.monotonic(), since, GapEnd(until)
        progress = ReplayProgress()
        try:
            for _, data in read_stream(since):
                try:
                    event = json.loads(data)
                    if is_canary(event):
                        continue
                    when = arrival_time(event)
                except (ValueError, KeyError, TypeError):
                    continue
                progress.add(when)
                if end.passed(when):
                    producer.flush()
                    finish_gap(0)
                    log.info("backfill: %s → %s UTC done, %d events recovered",
                             f"{since:%H:%M}", f"{until:%H:%M}", sent)
                    break
                if when < until:
                    frontier = max(frontier, when)
                    if keep_event(event):
                        send(producer, event, data)
                        sent += 1
                if time.monotonic() - last_log >= LOG_EVERY_S:
                    save_replay_progress(producer, progress)
                    log.info("backfill: at %s UTC, %.0f min to go, %d events recovered so far",
                             f"{frontier:%H:%M}", (until - frontier).total_seconds() / 60, sent)
                    last_log = time.monotonic()
        except requests.RequestException as exc:
            save_replay_progress(producer, progress)
            log.warning("backfill: stream dropped (%s); resuming from where it got to in 30 s", exc)
            time.sleep(30)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if not USER_AGENT:
        sys.exit("WIKI_USER_AGENT is not set: copy .env.example to .env and fill it in")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # `docker compose stop` -> clean shutdown

    producer = connect_producer()
    restart_from = load_position()   # time of the last event forwarded by the previous run
    log.info("Connected to Kafka at %s; live from now%s", BOOTSTRAP,
             f", and backfilling since {restart_from:%Y-%m-%d %H:%M:%S} UTC" if restart_from else "")
    threading.Thread(target=backfill_worker, args=(producer,), daemon=True, name="backfill").start()

    seen = kept = 0
    last_save = last_log = time.monotonic()
    backoff = 1
    continuity = StreamContinuity()   # kept across reconnects, so a reconnect's gap shows up as a jump
    try:
        while True:
            behind = Streak(100)
            try:
                for _, data in read_stream():
                    backoff = 1
                    seen += 1
                    try:
                        event = json.loads(data)
                        if is_canary(event):
                            continue
                        when = arrival_time(event)
                    except (ValueError, KeyError, TypeError):
                        continue
                    if keep_event(event):
                        send(producer, event, data)
                        kept += 1

                    if restart_from and continuity.last_time is None:
                        add_gap(restart_from, when)   # everything since the previous run stopped
                        restart_from = None
                    previous = continuity.last_time
                    if continuity.jumped(when):
                        log.warning("the live stream jumped from %s to %s UTC (laptop asleep or offline?); "
                                    "queued for backfill", f"{previous:%H:%M:%S}", f"{when:%H:%M:%S}")
                        add_gap(previous, when)
                    if behind.update(datetime.now(timezone.utc) - when > LIVE_MAX_LAG):
                        raise LiveFellBehind(when)

                    now = time.monotonic()
                    if now - last_save >= SAVE_EVERY_S:
                        producer.flush()  # only remember positions of events that are safely in Kafka
                        save_position(continuity.last_time)
                        last_save = now
                    if now - last_log >= LOG_EVERY_S:
                        log.info("last %.0f s: %d events read, %d sent to %s",
                                 now - last_log, seen, kept, RAW_TOPIC)
                        seen = kept = 0
                        last_log = now
            except LiveFellBehind as exc:
                log.warning("live connection is serving events from %s UTC (laptop slept?); reconnecting "
                            "from now, the missed stretch is queued for backfill", f"{exc.args[0]:%H:%M}")
            except requests.RequestException as exc:
                log.warning("Stream dropped (%s); reconnecting in %d s", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
    finally:
        producer.flush()
        save_position(continuity.last_time or restart_from)
        producer.close()


if __name__ == "__main__":
    main()
