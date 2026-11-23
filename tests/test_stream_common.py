import unittest
from datetime import datetime, timezone

from stream.common import SendChecker, SendFailed, event_time, hour_prefix, iter_sse, keep_event


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
    def test_gaps_are_time_ranges_and_old_id_entries_are_converted(self):
        import json
        import tempfile
        from datetime import datetime, timezone
        from pathlib import Path
        from stream import producer

        U = lambda h, m: datetime(2026, 10, 28, h, m, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            producer.GAPS_FILE = Path(tmp) / "pending_gaps.json"
            old_id = json.dumps([{"topic": "eqiad.mediawiki.recentchange", "partition": 0,
                                  "timestamp": int(U(12, 10).timestamp() * 1000)},
                                 {"topic": "codfw.mediawiki.recentchange", "partition": 0, "offset": -1}])
            producer.GAPS_FILE.write_text(json.dumps([{"from_id": old_id, "until": U(12, 41).isoformat()}]))
            producer.add_gap(U(13, 5), U(14, 19))
            gaps = producer.load_gaps()
            self.assertEqual([g["since"] for g in gaps], [U(12, 10).isoformat(), U(13, 5).isoformat()])
            producer.finish_gap(0)
            self.assertEqual(producer.load_gaps()[0]["until"], U(14, 19).isoformat())
        self.assertEqual(producer.since_param(U(12, 10)), "2026-10-28T12:10:00Z")


class ReplayProgressTest(unittest.TestCase):
    def test_a_dropped_replay_resumes_where_it_got_to_but_never_past_a_stray(self):
        import tempfile
        from datetime import datetime, timedelta, timezone
        from pathlib import Path
        from stream import producer

        U = lambda h, m: datetime(2026, 11, 2, h, m, tzinfo=timezone.utc)
        progress = producer.ReplayProgress(window=2, topics=("quiet", "busy"))
        progress.add(U(16, 0), "quiet")
        progress.add(U(23, 0), "quiet")                    # the first topic races ahead to now
        self.assertIsNone(progress.safe_point())           # the other has not started: nothing is safe
        progress.add(U(16, 1), "busy")
        self.assertEqual(progress.safe_point(), U(16, 0))
        progress.add(U(23, 30), "quiet")
        self.assertEqual(progress.safe_point(), U(16, 1))  # held back by the topic furthest behind
        progress.add(U(16, 2), "busy")
        progress.add(U(16, 3), "busy")
        self.assertEqual(progress.safe_point(), U(16, 2))

        with tempfile.TemporaryDirectory() as tmp:
            producer.GAPS_FILE = Path(tmp) / "pending_gaps.json"
            producer.add_gap(U(15, 26), U(7, 27) + timedelta(days=1))
            producer.advance_gap(U(16, 1))
            producer.advance_gap(U(15, 50))                # never backwards
            self.assertEqual(producer.load_gaps()[0]["since"], U(16, 1).isoformat())


class GapEndTest(unittest.TestCase):
    def test_every_topic_must_pass_the_end_of_the_gap(self):
        from datetime import datetime, timedelta, timezone
        from stream.producer import GapEnd

        until = datetime(2026, 11, 6, 0, 40, tzinfo=timezone.utc)
        end = GapEnd(until, needed=3, topics=("quiet", "busy"))
        old, late = until - timedelta(minutes=30), until + timedelta(hours=9)
        for _ in range(50):
            self.assertFalse(end.passed(late, "quiet"))  # 7 Nov: this alone ended gaps with nothing recovered
        self.assertFalse(end.passed(old, "busy"))
        self.assertFalse(end.passed(late, "busy"))       # one late event is not enough
        self.assertFalse(end.passed(old, "busy"))
        self.assertFalse(end.passed(late, "busy"))
        self.assertFalse(end.passed(late, "busy"))
        self.assertTrue(end.passed(late, "busy"))

    def test_a_silent_topic_does_not_hold_the_replay_forever(self):
        from datetime import datetime, timedelta, timezone
        from stream.producer import GapEnd

        until = datetime(2026, 11, 6, 0, 40, tzinfo=timezone.utc)
        end = GapEnd(until, needed=3, topics=("quiet", "busy"), give_up=10)
        self.assertFalse(any(end.passed(until + timedelta(minutes=1), "busy") for _ in range(9)))
        self.assertTrue(end.passed(until + timedelta(minutes=1), "busy"))


class LiveLagTest(unittest.TestCase):
    def test_reconnect_only_when_clearly_behind(self):
        from stream.producer import Streak

        behind = Streak(3)
        self.assertFalse(any(behind.update(old) for old in [True, True, False, True, True]))  # strays
        self.assertTrue(behind.update(True))  # third old event in a row: the connection is behind


class StreamContinuityTest(unittest.TestCase):
    def test_jumps_are_detected_and_old_strays_ignored(self):
        from datetime import datetime, timedelta, timezone
        from stream.producer import StreamContinuity, is_canary

        t0 = datetime(2026, 10, 28, 12, 11, tzinfo=timezone.utc)
        c = StreamContinuity()
        self.assertFalse(c.jumped(t0))
        self.assertFalse(c.jumped(t0 + timedelta(milliseconds=40)))         # normal spacing
        self.assertFalse(c.jumped(t0 - timedelta(hours=2)))                 # an old stray event
        self.assertTrue(c.jumped(t0 + timedelta(minutes=29)))               # 28 Oct: 29 min asleep
        self.assertFalse(c.jumped(t0 + timedelta(minutes=29, seconds=1)))
        self.assertTrue(is_canary({"meta": {"domain": "canary"}}))
        self.assertFalse(is_canary({"meta": {"domain": "en.wikipedia.org"}}))


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


class StatusHolesTest(unittest.TestCase):
    def test_empty_and_half_empty_hours_are_found_but_not_the_running_hour(self):
        from datetime import datetime, timedelta, timezone
        from stream.status import find_holes

        start = datetime(2026, 11, 3, 0, tzinfo=timezone.utc)
        hours = [start + timedelta(hours=i) for i in range(12)]
        events = {h: 70000 for h in hours}
        events[hours[1]] = 0
        events[hours[2]] = 22000                          # 3 Nov: a third of an hour, not a zero
        events[hours[3]] = 40000                          # a quiet hour is not a hole
        del events[hours[4]]                              # no file at all
        events[hours[11]] = 100                           # the running hour is still filling
        self.assertEqual(find_holes(events, start, hours[11]), ([hours[1], hours[4]], [hours[2]]))

    def test_counts_are_read_from_the_object_names(self):
        from datetime import datetime, timezone
        from stream.status import hourly_counts

        objects = [{"Key": "clean/edits/dt=2026-11-03/hour=19/part-1-a-n10.parquet", "Size": 5},
                   {"Key": "clean/edits/dt=2026-11-03/hour=19/part-2-b-n7.parquet", "Size": 3}]
        self.assertEqual(hourly_counts(objects), {datetime(2026, 11, 3, 19, tzinfo=timezone.utc): (17, 8)})


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



class _Future:
    """Stands in for kafka-python's FutureRecordMetadata: stores errbacks, fails on demand."""
    def __init__(self):
        self.errbacks = []

    def add_errback(self, fn):
        self.errbacks.append(fn)

    def fail(self, exc):
        for fn in self.errbacks:
            fn(exc)


class SendCheckerTest(unittest.TestCase):
    def test_passes_when_every_send_succeeded(self):
        sends = SendChecker()
        sends.watch(_Future())
        sends.check()  # no exception

    def test_a_failed_send_blocks_the_commit(self):
        sends = SendChecker()
        ok, bad = sends.watch(_Future()), sends.watch(_Future())
        bad.fail(TimeoutError("broker gone"))
        with self.assertRaises(SendFailed):
            sends.check()
        self.assertIsInstance(sends.error, TimeoutError)


if __name__ == "__main__":
    unittest.main()
