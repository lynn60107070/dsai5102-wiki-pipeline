import unittest
from datetime import datetime, timezone

from stream.common import event_time, hour_prefix, iter_sse, keep_event


class KeepEventTest(unittest.TestCase):
    def test_keeps_edits_and_new_pages(self):
        self.assertTrue(keep_event({"type": "edit", "meta": {"domain": "en.wikipedia.org"}}))
        self.assertTrue(keep_event({"type": "new", "meta": {"domain": "ar.wikipedia.org"}}))

    def test_drops_other_types_and_canary_events(self):
        self.assertFalse(keep_event({"type": "log", "meta": {"domain": "en.wikipedia.org"}}))
        self.assertFalse(keep_event({"type": "categorize", "meta": {"domain": "en.wikipedia.org"}}))
        self.assertFalse(keep_event({"type": "edit", "meta": {"domain": "canary"}}))
        self.assertFalse(keep_event({}))


class EventTimeTest(unittest.TestCase):
    def test_uses_edit_timestamp(self):
        self.assertEqual(event_time({"timestamp": 1761400800}),
                         datetime(2025, 10, 25, 14, 0, tzinfo=timezone.utc))

    def test_falls_back_to_meta_dt(self):
        self.assertEqual(event_time({"meta": {"dt": "2026-10-28T23:59:59Z"}}),
                         datetime(2026, 10, 28, 23, 59, 59, tzinfo=timezone.utc))


class HourPrefixTest(unittest.TestCase):
    def test_hive_style_partition_in_utc(self):
        when = datetime(2026, 10, 28, 7, 30, tzinfo=timezone.utc)
        self.assertEqual(hour_prefix(when), "raw/edits/dt=2026-10-28/hour=07")


class BackfillGapQueueTest(unittest.TestCase):
    def test_gaps_are_queued_progressed_and_removed(self):
        import tempfile
        from datetime import datetime, timezone
        from pathlib import Path
        from stream import producer

        with tempfile.TemporaryDirectory() as tmp:
            producer.GAPS_FILE = Path(tmp) / "pending_gaps.json"
            producer.add_gap("id-1", datetime(2026, 10, 27, 12, 22, tzinfo=timezone.utc))
            producer.add_gap("id-2", datetime(2026, 10, 27, 15, 0, tzinfo=timezone.utc))
            self.assertEqual([g["from_id"] for g in producer.load_gaps()], ["id-1", "id-2"])
            producer.update_gap(0, from_id="id-1b")          # progress survives a restart
            self.assertEqual(producer.load_gaps()[0]["from_id"], "id-1b")
            producer.update_gap(0, done=True)                # finished gaps leave the queue
            self.assertEqual([g["from_id"] for g in producer.load_gaps()], ["id-2"])
            self.assertEqual(producer.load_gaps()[0]["until"], "2026-10-27T15:00:00+00:00")


class GapEndTest(unittest.TestCase):
    def test_a_stray_late_event_does_not_end_the_backfill(self):
        from datetime import datetime, timedelta, timezone
        from stream.producer import GapEnd

        until = datetime(2026, 10, 27, 14, 57, tzinfo=timezone.utc)
        end = GapEnd(until, needed=3)
        old, late = until - timedelta(hours=2), until + timedelta(minutes=1)
        self.assertFalse(end.passed(old))
        self.assertFalse(end.passed(late))   # one stray late event (27 Oct: this ended it 2 h early)
        self.assertFalse(end.passed(old))    # ... and the replay is still in the gap
        self.assertFalse(end.passed(late))
        self.assertFalse(end.passed(late))
        self.assertTrue(end.passed(late))    # 3 in a row: really past the end


class LiveLagTest(unittest.TestCase):
    def test_reconnect_only_when_clearly_behind(self):
        from stream.producer import Streak

        behind = Streak(3)
        self.assertFalse(any(behind.update(old) for old in [True, True, False, True, True]))  # strays
        self.assertTrue(behind.update(True))  # third old event in a row: the connection is behind


class SafeCommitTest(unittest.TestCase):
    def test_a_commit_after_a_rebalance_is_logged_not_raised(self):
        import logging
        from kafka.errors import CommitFailedError
        from stream.common import safe_commit

        from kafka.errors import UnknownError

        class Failing:
            def __init__(self, error):
                self.error = error

            def commit(self):
                raise self.error

        class Healthy:
            def commit(self):
                pass

        log = logging.getLogger("test")
        for error in (CommitFailedError("evicted"), UnknownError()):
            with self.assertLogs(log, level="WARNING"):
                self.assertFalse(safe_commit(Failing(error), log))
        self.assertTrue(safe_commit(Healthy(), log))


class ObjectNameTest(unittest.TestCase):
    def test_event_count_round_trips_through_the_name(self):
        from stream import lake
        name = lake.part_name(1234)
        self.assertTrue(name.endswith("-n1234.jsonl.gz"))
        self.assertEqual(lake.count_from_name(f"raw/edits/dt=2026-10-28/hour=07/{name}"), 1234)
        self.assertIsNone(lake.count_from_name("raw/edits/dt=2026-10-28/hour=07/part-1-host.jsonl.gz"))
        self.assertEqual(lake.count_from_name(lake.part_name(7, suffix=".parquet")), 7)


class IterSseTest(unittest.TestCase):
    def test_parses_events_and_skips_comments(self):
        lines = [
            ":ok", "",
            "event: message", 'id: [{"offset":1}]', 'data: {"a": 1}', "",
            "event: message", 'data: {"b": 2}', "",
            'data: {"unfinished": true}',
        ]
        self.assertEqual(list(iter_sse(lines)), [('[{"offset":1}]', '{"a": 1}'), (None, '{"b": 2}')])


if __name__ == "__main__":
    unittest.main()
