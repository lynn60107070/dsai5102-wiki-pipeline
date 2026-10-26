"""Live stats sink: `edits.per_minute` -> Postgres `serving.live_edits_per_minute` (near-real-time tile).

Keeps the last 48 hours. A row for a (minute, wiki) that already exists is *added* to it: after a
processor restart, the new processor only sees the events the old one had not counted yet, so adding
completes the minute (replacing would overwrite the full count with the small remainder). The rare
cost: a crash between writing and committing can count a few events twice. Metabase reads the table
with the read-only user, like the marts; the warehouse, not this live view, is the source of truth.
Run inside Docker: `python -m stream.live_stats`
"""
import json
import logging
import os
import signal
import sys
import time

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from stream.common import BOOTSTRAP, CONSUMER_SETTINGS, safe_commit

TOPIC = "edits.per_minute"
GROUP_ID = os.environ.get("LIVE_STATS_GROUP", "live-stats")
KEEP_HOURS = 48

CREATE = """
CREATE TABLE IF NOT EXISTS live_edits_per_minute (
    minute_ts   timestamptz NOT NULL,
    wiki_id     text        NOT NULL,
    bot_edits   integer     NOT NULL,
    human_edits integer     NOT NULL,
    PRIMARY KEY (minute_ts, wiki_id)
)"""
UPSERT = """
INSERT INTO live_edits_per_minute (minute_ts, wiki_id, bot_edits, human_edits)
VALUES (to_timestamp(%(minute_ts)s), %(wiki_id)s, %(bot_edits)s, %(human_edits)s)
ON CONFLICT (minute_ts, wiki_id) DO UPDATE
SET bot_edits = live_edits_per_minute.bot_edits + EXCLUDED.bot_edits,
    human_edits = live_edits_per_minute.human_edits + EXCLUDED.human_edits"""
PRUNE = f"DELETE FROM live_edits_per_minute WHERE minute_ts < now() - interval '{KEEP_HOURS} hours'"

log = logging.getLogger("live_stats")


def connect_consumer() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP, group_id=GROUP_ID,
                                 enable_auto_commit=False, auto_offset_reset="earliest", **CONSUMER_SETTINGS,
                                 value_deserializer=json.loads)
        except NoBrokersAvailable:
            log.info("Kafka not reachable at %s yet, retrying in 5 s", BOOTSTRAP)
            time.sleep(5)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    db = psycopg2.connect(host=os.environ.get("POSTGRES_HOST", "postgres"), dbname="serving",
                          user="airflow", password=os.environ["POSTGRES_PASSWORD"])
    with db, db.cursor() as cur:
        cur.execute(CREATE)
    consumer = connect_consumer()
    log.info("Reading %s as group %s into serving.live_edits_per_minute", TOPIC, GROUP_ID)
    last_prune = 0.0
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000, max_records=1000)
            rows = [record.value for records in batch.values() for record in records]
            if rows:
                with db, db.cursor() as cur:
                    cur.executemany(UPSERT, rows)
                safe_commit(consumer, log)
            if time.monotonic() - last_prune > 600:
                with db, db.cursor() as cur:
                    cur.execute(PRUNE)
                last_prune = time.monotonic()
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
