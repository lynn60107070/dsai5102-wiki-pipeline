"""Producer: Wikimedia EventStreams (recentchange) -> Kafka topic `wiki.raw`.

Keeps only edits and page creations, keys each event by wiki (so one wiki always lands in the same
partition, in order), and stores the last event id so that after a restart it resumes where it
stopped, as long as Wikimedia still holds those events.

Run inside Docker: `python -m stream.producer`
"""
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from stream.common import BOOTSTRAP, RAW_TOPIC, iter_sse, keep_event

STREAM_URL = os.environ.get("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
USER_AGENT = os.environ.get("WIKI_USER_AGENT", "")  # Wikimedia's API policy requires a descriptive one
STATE_FILE = Path(os.environ.get("STATE_DIR", "/state")) / "last_event_id"
SAVE_EVERY_S = 10
LOG_EVERY_S = 60

log = logging.getLogger("producer")


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


def load_last_id():
    if not STATE_FILE.exists():
        return None
    return STATE_FILE.read_text().strip() or None


def save_last_id(last_id):
    if not last_id:
        return
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(last_id)
    tmp.replace(STATE_FILE)


def read_stream(last_id):
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_id:
        headers["Last-Event-ID"] = last_id
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as response:
        response.raise_for_status()
        response.encoding = "utf-8"
        yield from iter_sse(response.iter_lines(decode_unicode=True))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if not USER_AGENT:
        sys.exit("WIKI_USER_AGENT is not set: copy .env.example to .env and fill it in")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # `docker compose stop` -> clean shutdown

    producer = connect_producer()
    last_id = load_last_id()
    log.info("Connected to Kafka at %s; %s", BOOTSTRAP,
             "resuming from the saved event id" if last_id else "starting from now")

    seen = kept = 0
    last_save = last_log = time.monotonic()
    backoff = 1
    try:
        while True:
            try:
                for event_id, data in read_stream(last_id):
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
