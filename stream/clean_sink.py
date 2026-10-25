"""Clean sink: `edits.bot` + `edits.human` -> Parquet files in the clean zone of the lake.

Runs as a consumer group (2 replicas in docker-compose.yml): Kafka splits the topics' partitions
between the replicas, so adding a replica spreads the load without any code change.

    s3://lake/clean/edits/dt=YYYY-MM-DD/hour=HH/part-<ms>-<host>-n<events>.parquet

Same guarantees as the raw sink: one object per hour per flush, offsets committed only after upload.
Run inside Docker: `python -m stream.clean_sink`
"""
import io
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from stream import lake
from stream.common import BOOTSTRAP, CONSUMER_SETTINGS

TOPICS = ("edits.bot", "edits.human")
GROUP_ID = os.environ.get("CLEAN_SINK_GROUP", "clean-archive")
FLUSH_SECONDS = int(os.environ.get("FLUSH_SECONDS", "300"))

SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("recent_change_id", pa.int64()),
    ("revision_id", pa.int64()),
    ("event_ts", pa.timestamp("s", tz="UTC")),
    ("wiki_id", pa.string()),
    ("server_name", pa.string()),
    ("namespace", pa.int32()),
    ("title", pa.string()),
    ("is_new_page", pa.bool_()),
    ("is_bot", pa.bool_()),
    ("is_minor", pa.bool_()),
    ("editor_type", pa.string()),
    ("editor_key", pa.string()),
    ("bytes_changed", pa.int64()),
    ("is_revert", pa.bool_()),
])

log = logging.getLogger("clean_sink")


def connect_consumer() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(*TOPICS, bootstrap_servers=BOOTSTRAP, group_id=GROUP_ID,
                                 enable_auto_commit=False, auto_offset_reset="earliest", **CONSUMER_SETTINGS,
                                 value_deserializer=json.loads)
        except NoBrokersAvailable:
            log.info("Kafka not reachable at %s yet, retrying in 5 s", BOOTSTRAP)
            time.sleep(5)


def to_parquet(events: list) -> bytes:
    columns = {field.name: [e.get(field.name) for e in events] for field in SCHEMA}
    columns["event_ts"] = [datetime.fromtimestamp(ts, tz=timezone.utc) for ts in columns["event_ts"]]
    buffer = io.BytesIO()
    pq.write_table(pa.table(columns, schema=SCHEMA), buffer, compression="zstd")
    return buffer.getvalue()


def flush(consumer, s3, buffers):
    if not buffers:
        return
    for prefix, events in buffers.items():
        key = f"{prefix}/{lake.part_name(len(events), suffix='.parquet')}"
        s3.put_object(Bucket=lake.BUCKET, Key=key, Body=to_parquet(events),
                      ContentType="application/vnd.apache.parquet")
    consumer.commit()
    log.info("wrote %d events into %d hour folder(s)", sum(map(len, buffers.values())), len(buffers))
    buffers.clear()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    s3 = lake.client()
    consumer = connect_consumer()
    log.info("Reading %s as group %s; writing Parquet to s3://%s/clean/edits every %d s",
             ", ".join(TOPICS), GROUP_ID, lake.BUCKET, FLUSH_SECONDS)
    buffers = defaultdict(list)
    last_flush = time.monotonic()
    try:
        while True:
            for records in consumer.poll(timeout_ms=1000, max_records=2000).values():
                for record in records:
                    when = datetime.fromtimestamp(record.value["event_ts"], tz=timezone.utc)
                    buffers[f"clean/edits/dt={when:%Y-%m-%d}/hour={when:%H}"].append(record.value)
            if time.monotonic() - last_flush >= FLUSH_SECONDS:
                flush(consumer, s3, buffers)
                last_flush = time.monotonic()
    finally:
        flush(consumer, s3, buffers)
        consumer.close()


if __name__ == "__main__":
    main()
