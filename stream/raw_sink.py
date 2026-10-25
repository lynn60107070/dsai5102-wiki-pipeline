"""Raw sink: Kafka topic `wiki.raw` -> raw zone of the lake (MinIO), one gzipped JSON-lines object per flush.

Events are grouped by the UTC hour they happened in. Every FLUSH_SECONDS, each hour's buffer is uploaded
as a new object under raw/edits/dt=YYYY-MM-DD/hour=HH/ (an S3 object only appears once it is complete,
so a crash never leaves a half-written file), and only then are the Kafka offsets committed. A crash can
therefore repeat events but never lose them; dbt removes the duplicates by event id.

Run inside Docker: `python -m stream.raw_sink`
"""
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from stream import lake
from stream.common import BOOTSTRAP, RAW_TOPIC, event_time, hour_prefix

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


def flush(consumer, s3, buffers):
    if not buffers:
        return
    for prefix, lines in buffers.items():
        lake.put_lines(s3, f"{prefix}/{lake.part_name(len(lines))}", lines)
    consumer.commit()
    log.info("wrote %d events into %d hour folder(s)", sum(map(len, buffers.values())), len(buffers))
    buffers.clear()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # `docker compose stop` -> flush, then exit

    s3 = lake.client()
    consumer = connect_consumer()
    log.info("Reading %s as group %s; writing to s3://%s every %d s", RAW_TOPIC, GROUP_ID, lake.BUCKET, FLUSH_SECONDS)
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
                    buffers[hour_prefix(when)].append(record.value)
            if time.monotonic() - last_flush >= FLUSH_SECONDS:
                flush(consumer, s3, buffers)
                last_flush = time.monotonic()
    finally:
        flush(consumer, s3, buffers)
        consumer.close()


if __name__ == "__main__":
    main()
