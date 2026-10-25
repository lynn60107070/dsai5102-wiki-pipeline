"""Raw sink: Kafka topic `wiki.raw` -> hourly gzipped JSON-lines files (the raw zone of the lake).

Events are grouped by the UTC hour they happened in. Every FLUSH_SECONDS, each hour's buffer is
written as a new file (first under a .tmp name, then renamed, so a crash never leaves a half-written
file), and only then are the Kafka offsets committed. A crash can therefore repeat events but never
lose them; dbt removes the duplicates by event id.

Run inside Docker: `python -m stream.raw_sink`
"""
import gzip
import json
import logging
import os
import signal
import socket
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from stream.common import BOOTSTRAP, RAW_TOPIC, event_time, hour_dir

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
FLUSH_SECONDS = int(os.environ.get("FLUSH_SECONDS", "300"))
GROUP_ID = os.environ.get("SINK_GROUP", "raw-archive")

log = logging.getLogger("raw_sink")


def connect_consumer() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
        except NoBrokersAvailable:
            log.info("Kafka not reachable at %s yet, retrying in 5 s", BOOTSTRAP)
            time.sleep(5)


def write_files(buffers) -> int:
    """Write each hour's buffered events to a new .jsonl.gz file in that hour's folder."""
    name = f"part-{int(time.time() * 1000)}-{socket.gethostname()}.jsonl.gz"
    for directory, lines in buffers.items():
        directory.mkdir(parents=True, exist_ok=True)
        final = directory / name
        tmp = final.with_name(final.name + ".tmp")
        with gzip.open(tmp, "wb") as f:
            f.write(b"\n".join(lines) + b"\n")
        tmp.replace(final)
    return sum(len(lines) for lines in buffers.values())


def flush(consumer, buffers):
    if not buffers:
        return
    written = write_files(buffers)
    consumer.commit()
    log.info("wrote %d events into %d hour folder(s)", written, len(buffers))
    buffers.clear()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # `docker compose stop` -> flush, then exit

    consumer = connect_consumer()
    log.info("Reading %s as group %s; writing to %s every %d s", RAW_TOPIC, GROUP_ID, DATA_DIR, FLUSH_SECONDS)
    buffers = defaultdict(list)
    last_flush = time.monotonic()
    try:
        while True:
            for records in consumer.poll(timeout_ms=1000, max_records=2000).values():
                for record in records:
                    try:
                        when = event_time(json.loads(record.value))
                    except (ValueError, KeyError, TypeError):
                        when = datetime.fromtimestamp(record.timestamp / 1000, tz=timezone.utc)
                    buffers[hour_dir(DATA_DIR, when)].append(record.value)
            if time.monotonic() - last_flush >= FLUSH_SECONDS:
                flush(consumer, buffers)
                last_flush = time.monotonic()
    finally:
        flush(consumer, buffers)
        consumer.close()


if __name__ == "__main__":
    main()
