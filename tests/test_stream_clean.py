import unittest

from stream.clean import InvalidEvent, MinuteCounter, clean_event, editor_type, is_revert

RAW = {
    "meta": {"id": "abc-123", "domain": "en.wikipedia.org"},
    "id": 42, "type": "edit", "namespace": 0, "title": "Doha", "comment": "Undid revision 1 by X",
    "timestamp": 1792915200, "user": "SomeEditor", "bot": False, "minor": True,
    "length": {"old": 100, "new": 80}, "revision": {"old": 1, "new": 2},
    "server_name": "en.wikipedia.org", "wiki": "enwiki",
}


class EditorTypeTest(unittest.TestCase):
    def test_types(self):
        self.assertEqual(editor_type("AnyBot", True), "bot")
        self.assertEqual(editor_type("~2025-31337-12", False), "anonymous")
        self.assertEqual(editor_type("192.168.0.1", False), "anonymous")
        self.assertEqual(editor_type("2001:db8::1", False), "anonymous")
        self.assertEqual(editor_type("Jimbo Wales", False), "registered")


class RevertTest(unittest.TestCase):
    def test_heuristic(self):
        self.assertTrue(is_revert("Undid revision 123 by Foo"))
        self.assertTrue(is_revert("Reverted edits by X to last version"))
        self.assertTrue(is_revert("rv vandalism"))
        self.assertFalse(is_revert("Fixed a typo in the reverse section"))
        self.assertFalse(is_revert(None))


class CleanEventTest(unittest.TestCase):
    def test_clean_event_drops_personal_fields(self):
        clean = clean_event(RAW)
        self.assertEqual(clean["event_id"], "abc-123")
        self.assertEqual(clean["bytes_changed"], -20)
        self.assertTrue(clean["is_revert"])
        self.assertEqual(clean["editor_type"], "registered")
        self.assertEqual(len(clean["editor_key"]), 32)
        self.assertNotIn("user", clean)
        self.assertNotIn("comment", clean)

    def test_invalid_events_raise_with_a_reason(self):
        for broken, reason in [({**RAW, "meta": {}}, "meta.id"), ({**RAW, "type": "log"}, "type"),
                               ({**RAW, "timestamp": "x"}, "timestamp"), ({**RAW, "bot": None}, "bot")]:
            with self.assertRaisesRegex(InvalidEvent, reason):
                clean_event(broken)


class MinuteCounterTest(unittest.TestCase):
    def test_emits_closed_minutes_only_and_drops_late_events(self):
        counter = MinuteCounter(lateness_s=120)
        counter.add(1000 * 60 + 5, "enwiki", True)
        counter.add(1000 * 60 + 10, "enwiki", False)
        counter.add(1000 * 60 + 20, "arwiki", False)
        self.assertEqual(counter.pop_closed(), [])  # minute 1000 is still open
        counter.add(1003 * 60 + 30, "enwiki", True)  # time moves on: minute 1000 closes
        self.assertEqual(counter.pop_closed(), [(60000, "arwiki", 0, 1), (60000, "enwiki", 1, 1)])
        counter.add(1000 * 60 + 50, "enwiki", True)  # too late for minute 1000
        self.assertEqual(counter.late_events, 1)

    def test_a_lagging_partition_holds_the_window_open(self):
        counter = MinuteCounter(lateness_s=120)
        counter.add(1000 * 60, "enwiki", True, source=0)
        counter.add(2000 * 60, "dewiki", True, source=1)   # partition 1 races ahead (catch-up)
        self.assertEqual(counter.pop_closed(sources=[0, 1]), [])
        counter.add(1000 * 60 + 30, "enwiki", False, source=0)  # partition 0 is not late
        self.assertEqual(counter.late_events, 0)
        counter.add(1005 * 60, "enwiki", False, source=0)  # partition 0 moves on: minute 1000 closes
        self.assertEqual(counter.pop_closed(sources=[0, 1]), [(60000, "enwiki", 1, 1)])

    def test_an_assigned_partition_with_no_data_yet_blocks_emission(self):
        counter = MinuteCounter(lateness_s=120)
        counter.add(2000 * 60, "enwiki", True, source=0)
        self.assertEqual(counter.pop_closed(sources=[0, 1, 2]), [])


if __name__ == "__main__":
    unittest.main()
