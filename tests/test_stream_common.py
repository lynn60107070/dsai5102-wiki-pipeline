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
