"""Producer: Wikimedia EventStreams (recentchange) -> Kafka topic `wiki.raw`.

Keeps only edits and page creations and keys each event by wiki (so one wiki always lands in the same
partition, in order). Two connections, so a backfill can never hold back live data:

  live      always connects from *now* (fast), and saves the id of the last event it forwarded
  backfill  when the live connection (re)starts after having had a position (a restart, the laptop
            sleeping, a network drop), the stretch from that position to the reconnect time is queued
            as a gap. A background thread replays each gap from the saved id and stops when it reaches
            the reconnect time. Gaps are persisted, so a restart mid-backfill carries on.

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
from datetime import datetime, timezone
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
        sent, last_log, last_save, frontier = 0, time.monotonic(), time.monotonic(), None
        try:
            for event_id, data in read_stream(gap["from_id"]):
                try:
                    event = json.loads(data)
                    frontier = arrival_time(event)
                except (ValueError, KeyError, TypeError):
                    continue
                if frontier >= until:
                    producer.flush()
                    update_gap(0, done=True)
                    log.info("backfill: gap up to %s UTC done, %d events recovered", f"{until:%H:%M}", sent)
                    break
                if keep_event(event):
                    producer.send(RAW_TOPIC, key=event.get("wiki", "unknown").encode(), value=data.encode())
                    sent += 1
                now = time.monotonic()
                if event_id and now - last_save >= SAVE_EVERY_S:
                    producer.flush()
                    update_gap(0, from_id=event_id)
                    last_save = now
                if now - last_log >= LOG_EVERY_S:
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
    last_id = load_last_id()
    log.info("Connected to Kafka at %s; live from now%s", BOOTSTRAP,
             ", and backfilling since the saved position" if last_id else "")
    threading.Thread(target=backfill_worker, args=(producer,), daemon=True, name="backfill").start()

    seen = kept = 0
    last_save = last_log = time.monotonic()
    backoff = 1
    try:
        while True:
            if last_id:  # we had a position: everything from there to this reconnect is a gap
                add_gap(last_id, datetime.now(timezone.utc))
                last_id = None
            try:
                for event_id, data in read_stream():
                    backoff = 1
                    seen += 1
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    if event_id:
                        last_id = event_id
                    if keep_event(event):
                        key = event.get("wiki", "unknown").encode()
                        producer.send(RAW_TOPIC, key=key, value=data.encode())
                        kept += 1

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
            except requests.RequestException as exc:
                log.warning("Stream dropped (%s); reconnecting in %d s", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
    finally:
        producer.flush()
        save_last_id(last_id)
        producer.close()


if __name__ == "__main__":
    main()
