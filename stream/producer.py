"""Producer: Wikimedia EventStreams (recentchange) -> Kafka topic `wiki.raw`.

Keeps only edits and page creations and keys each event by wiki (so one wiki always lands in the same
partition, in order). Two connections, so a backfill can never hold back live data:

  live      always connects from *now* (fast), and saves the id of the last event it forwarded. Any
            jump of more than 3 s between consecutive events (laptop asleep, offline, a dropped
            connection) and, after a restart, everything since the saved position, is queued as a gap
  backfill  a background thread replays each gap from the id just before it and stops when it reaches
            the time the live stream resumed. Gaps are persisted, so a restart mid-backfill carries on.

Wikimedia's hourly "canary" test events carry arbitrary timestamps and are ignored by all time logic.

Why: on 27 Oct Wikimedia served every resumed ("historical") connection at ~5% speed, even one only
seconds old, while fresh live connections were fast. With a single connection a resume held back live
data for hours. Overlaps between the two only create duplicates, which compaction and dbt remove.

Run inside Docker: `python -m stream.producer`
"""
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from stream.common import BOOTSTRAP, RAW_TOPIC, iter_sse, keep_event

STREAM_URL = os.environ.get("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
USER_AGENT = os.environ.get("WIKI_USER_AGENT", "")  # Wikimedia's API policy requires a descriptive one
STATE_DIR = Path(os.environ.get("STATE_DIR", "/state"))
STATE_FILE = STATE_DIR / "last_event_id"
GAPS_FILE = STATE_DIR / "pending_gaps.json"
SAVE_EVERY_S = 10
LOG_EVERY_S = 60

log = logging.getLogger("producer")
_gaps_lock = threading.Lock()


def connect_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                acks="all",
                compression_type="gzip",
                linger_ms=200,
                retries=5,
            )
        except NoBrokersAvailable:
            log.info("Kafka not reachable at %s yet, retrying in 5 s", BOOTSTRAP)
            time.sleep(5)


def _write_atomic(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text)
    tmp.replace(path)


def load_last_id():
    if not STATE_FILE.exists():
        return None
    return STATE_FILE.read_text().strip() or None


def save_last_id(last_id):
    if last_id:
        _write_atomic(STATE_FILE, last_id)


def load_gaps() -> list:
    with _gaps_lock:
        return json.loads(GAPS_FILE.read_text()) if GAPS_FILE.exists() else []


def add_gap(from_id: str, until: datetime):
    with _gaps_lock:
        gaps = json.loads(GAPS_FILE.read_text()) if GAPS_FILE.exists() else []
        gaps.append({"from_id": from_id, "until": until.isoformat()})
        _write_atomic(GAPS_FILE, json.dumps(gaps))


def update_gap(index: int, from_id=None, done=False):
    """Record backfill progress (so a restart resumes from here), or remove a finished gap."""
    with _gaps_lock:
        gaps = json.loads(GAPS_FILE.read_text()) if GAPS_FILE.exists() else []
        if index >= len(gaps):
            return
        if done:
            gaps.pop(index)
        elif from_id:
            gaps[index]["from_id"] = from_id
        _write_atomic(GAPS_FILE, json.dumps(gaps))


def read_stream(last_id=None):
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_id:
        headers["Last-Event-ID"] = last_id
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as response:
        response.raise_for_status()
        response.encoding = "utf-8"
        yield from iter_sse(response.iter_lines(decode_unicode=True))


def arrival_time(event: dict):
    """When Wikimedia published the event (meta.dt): increases along the stream, unlike edit times."""
    return datetime.fromisoformat(event["meta"]["dt"].replace("Z", "+00:00"))


class Streak:
    """True once a condition has held for `needed` events in a row. The stream interleaves occasional
    events with very different timestamps, so decisions based on a single event misfire (27 Oct)."""

    def __init__(self, needed: int):
        self.needed, self.run = needed, 0

    def update(self, condition: bool) -> bool:
        self.run = self.run + 1 if condition else 0
        return self.run >= self.needed


class GapEnd:
    """Has the replay passed the end of the gap? Only after 200 events in a row at or after `until`
    (stopping at the first one ended a backfill 2 hours early, 27 Oct)."""

    def __init__(self, until: datetime, needed: int = 200):
        self.until, self.streak = until, Streak(needed)

    def passed(self, when: datetime) -> bool:
        return self.streak.update(when >= self.until)


class LiveFellBehind(Exception):
    """The live connection is serving old events (e.g. it survived the laptop sleeping): reconnect."""


LIVE_MAX_LAG = timedelta(minutes=3)


def is_canary(event: dict) -> bool:
    """Wikimedia's hourly test events: arbitrary timestamps (a replay starts with ones stamped hours
    ahead), so they must never drive any time logic. They were already never stored."""
    return event.get("meta", {}).get("domain") == "canary"


class StreamContinuity:
    """Detects a jump in the live stream: consecutive real events are normally milliseconds apart, so a
    jump of more than `max_jump` means events were missed in between, whatever the cause (laptop asleep,
    offline, a dropped connection, 28 Oct). Old stray events never move the clock backwards."""

    def __init__(self, max_jump: timedelta = timedelta(seconds=3)):
        self.max_jump, self.last_time = max_jump, None

    def jumped(self, when: datetime) -> bool:
        jump = self.last_time is not None and when - self.last_time > self.max_jump
        if self.last_time is None or when > self.last_time:
            self.last_time = when
        return jump


def backfill_worker(producer: KafkaProducer):
    """Replay queued gaps one at a time, oldest first, at whatever speed Wikimedia serves them."""
    while True:
        gaps = load_gaps()
        if not gaps:
            time.sleep(10)
            continue
        gap = gaps[0]
        until = datetime.fromisoformat(gap["until"])
        log.info("backfill: replaying the gap up to %s UTC (%d gap(s) queued)", f"{until:%Y-%m-%d %H:%M}", len(gaps))
        sent, last_log, frontier = 0, time.monotonic(), None
        end = GapEnd(until)
        try:
            for event_id, data in read_stream(gap["from_id"]):
                try:
                    event = json.loads(data)
                    if is_canary(event):
                        continue
                    when = arrival_time(event)
                except (ValueError, KeyError, TypeError):
                    continue
                if when < until:
                    frontier = when
                if end.passed(when):
                    producer.flush()
                    update_gap(0, done=True)
                    log.info("backfill: gap up to %s UTC done, %d events recovered", f"{until:%H:%M}", sent)
                    break
                if keep_event(event):
                    producer.send(RAW_TOPIC, key=event.get("wiki", "unknown").encode(), value=data.encode())
                    sent += 1
                # No mid-gap progress is saved: the id of the last event read can belong to a stray late
                # event and would make a restart skip the rest of the gap (27 Oct). After a restart the
                # gap is replayed from its start; the duplicates are removed downstream.
                now = time.monotonic()
                if now - last_log >= LOG_EVERY_S and frontier:
                    log.info("backfill: at %s UTC, %.0f min to go, %d events recovered so far",
                             f"{frontier:%H:%M}", (until - frontier).total_seconds() / 60, sent)
                    last_log = now
        except requests.RequestException as exc:
            log.warning("backfill: stream dropped (%s); retrying in 30 s", exc)
            time.sleep(30)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if not USER_AGENT:
        sys.exit("WIKI_USER_AGENT is not set: copy .env.example to .env and fill it in")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # `docker compose stop` -> clean shutdown

    producer = connect_producer()
    restart_from = load_last_id()   # position saved by the previous run: the gap since then is queued
    log.info("Connected to Kafka at %s; live from now%s", BOOTSTRAP,
             ", and backfilling since the saved position" if restart_from else "")
    threading.Thread(target=backfill_worker, args=(producer,), daemon=True, name="backfill").start()

    seen = kept = 0
    last_save = last_log = time.monotonic()
    backoff = 1
    last_id = None
    continuity = StreamContinuity()   # kept across reconnects, so a reconnect's gap shows up as a jump
    try:
        while True:
            behind = Streak(100)
            try:
                for event_id, data in read_stream():
                    backoff = 1
                    seen += 1
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    previous_id = last_id
                    if event_id:
                        last_id = event_id
                    if keep_event(event):
                        key = event.get("wiki", "unknown").encode()
                        producer.send(RAW_TOPIC, key=key, value=data.encode())
                        kept += 1
                    if is_canary(event):
                        continue
                    try:
                        when = arrival_time(event)
                    except (KeyError, ValueError, TypeError):
                        continue
                    # Gaps: after a restart, everything since the saved position; while running, any jump
                    # in the stream (sleep, offline, reconnect). The backfill thread replays them.
                    if restart_from and continuity.last_time is None:
                        add_gap(restart_from, when)
                        restart_from = None
                        continuity.jumped(when)
                    elif continuity.jumped(when) and previous_id:
                        log.warning("the live stream jumped to %s UTC (laptop asleep or offline?); the missed "
                                    "stretch is queued for backfill", f"{when:%H:%M:%S}")
                        add_gap(previous_id, when)
                    # A live connection that survived the laptop sleeping can keep serving the backlog
                    # slowly (27 Oct). If it is clearly behind, reconnect from now (the jump is then queued).
                    if behind.update(datetime.now(timezone.utc) - when > LIVE_MAX_LAG):
                        raise LiveFellBehind(when)

                    now = time.monotonic()
                    if now - last_save >= SAVE_EVERY_S:
                        producer.flush()  # only remember ids of events that are safely in Kafka
                        save_last_id(last_id)
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
        save_last_id(last_id or restart_from)
        producer.close()


if __name__ == "__main__":
    main()
