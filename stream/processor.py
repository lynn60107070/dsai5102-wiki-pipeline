"""Stream processor: `wiki.raw` -> validated, privacy-safe events split by editor, plus per-minute counts.

    wiki.raw ──► edits.bot         clean events from bot accounts      (key = wiki)
             ├─► edits.human       clean events from everyone else     (key = wiki)
             ├─► edits.per_minute  edits per wiki and minute, bot/human (key = wiki)
             └─► wiki.deadletter   events that failed validation, with the reason

Starts from the oldest event Kafka still holds (3 days) the first time, so the clean zone covers the
history too. Offsets are committed after the outputs are flushed: at-least-once, like the sinks.
Run inside Docker: `python -m stream.processor`
"""
import json
import logging
import os
import signal
import sys
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from stream.clean import InvalidEvent, MinuteCounter, clean_event
from stream.common import BOOTSTRAP, CONSUMER_SETTINGS, RAW_TOPIC, safe_commit

GROUP_ID = os.environ.get("PROCESSOR_GROUP", "processor")
BOT_TOPIC, HUMAN_TOPIC = "edits.bot", "edits.human"
MINUTE_TOPIC, DEADLETTER_TOPIC = "edits.per_minute", "wiki.deadletter"
LOG_EVERY_S = 60

log = logging.getLogger("processor")


def connect():
    while True:
        try:
            consumer = KafkaConsumer(RAW_TOPIC, bootstrap_servers=BOOTSTRAP, group_id=GROUP_ID,
                                     enable_auto_commit=False, auto_offset_reset="earliest", **CONSUMER_SETTINGS)
            producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", compression_type="gzip",
                                     linger_ms=200, value_serializer=lambda v: json.dumps(v).encode())
            return consumer, producer
        except NoBrokersAvailable:
            log.info("Kafka not reachable at %s yet, retrying in 5 s", BOOTSTRAP)
            time.sleep(5)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    consumer, producer = connect()
    minutes = MinuteCounter()
    stats = {"bot": 0, "human": 0, "deadletter": 0, "minute_rows": 0}
    last_log = time.monotonic()
    log.info("Reading %s as group %s", RAW_TOPIC, GROUP_ID)
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000, max_records=2000)
            for records in batch.values():
                for record in records:
                    try:
                        event = clean_event(json.loads(record.value))
                    except (InvalidEvent, ValueError) as exc:
                        producer.send(DEADLETTER_TOPIC, {"reason": str(exc),
                                                         "raw": record.value.decode("utf-8", "replace")})
                        stats["deadletter"] += 1
                        continue
                    key = event["wiki_id"].encode()
                    producer.send(BOT_TOPIC if event["is_bot"] else HUMAN_TOPIC, event, key=key)
                    stats["bot" if event["is_bot"] else "human"] += 1
                    minutes.add(event["event_ts"], event["wiki_id"], event["is_bot"], source=record.partition)

            assigned = [tp.partition for tp in consumer.assignment()]
            for minute_ts, wiki, bot_edits, human_edits in minutes.pop_closed(sources=assigned):
                producer.send(MINUTE_TOPIC, {"minute_ts": minute_ts, "wiki_id": wiki,
                                             "bot_edits": bot_edits, "human_edits": human_edits},
                              key=wiki.encode())
                stats["minute_rows"] += 1

            if batch:
                producer.flush()   # outputs are safely in Kafka before the input offsets move on
                safe_commit(consumer, log)

            if time.monotonic() - last_log >= LOG_EVERY_S:
                log.info("last %.0f s: %d bot, %d human, %d dead-letter events; %d per-minute rows; %d late",
                         time.monotonic() - last_log, stats["bot"], stats["human"], stats["deadletter"],
                         stats["minute_rows"], minutes.late_events)
                stats = dict.fromkeys(stats, 0)
                last_log = time.monotonic()
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
