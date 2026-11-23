import re
import unittest

from scripts.metabase_setup import CARDS, FILTERS, NOTE_KEY, dashboard_body, parameter_mappings, template_tags

TAG = re.compile(r"\{\{\s*(\w+)\s*\}\}")


class CardFilterTest(unittest.TestCase):
    def test_every_variable_in_the_sql_is_declared_and_every_declared_tag_is_used(self):
        for card in CARDS:
            with self.subTest(card=card["name"]):
                self.assertEqual(set(TAG.findall(card["sql"])), set(card.get("tags", {})))

    def test_filter_clauses_are_optional_so_a_tile_works_with_no_filter_set(self):
        for card in CARDS:
            outside = re.sub(r"\[\[.*?\]\]", "", card["sql"], flags=re.S)
            with self.subTest(card=card["name"]):
                self.assertEqual(TAG.findall(outside), [])

    def test_each_declared_tag_belongs_to_a_dashboard_filter(self):
        filter_tags = {f["tag"] for f in FILTERS}
        for card in CARDS:
            with self.subTest(card=card["name"]):
                self.assertLessEqual(set(card.get("tags", {})), filter_tags)

    def test_text_filters_allow_one_value_only(self):
        # a text variable is pasted into `= {{tag}}`, so a multi-select list would be a SQL error
        for f in FILTERS:
            if f["tag_type"] == "text":
                with self.subTest(filter=f["name"]):
                    self.assertIs(f.get("isMultiSelect"), False)

    def test_template_tags_have_the_shape_metabase_expects(self):
        for card in CARDS:
            for name, tag in template_tags(card).items():
                with self.subTest(card=card["name"], tag=name):
                    self.assertEqual(tag["name"], name)
                    self.assertIn(tag["type"], ("date", "text"))
                    self.assertFalse(tag["required"])

    def test_mappings_only_cover_filters_the_card_uses(self):
        for card in CARDS:
            mappings = parameter_mappings(card, card_id=7)
            used = {m["target"][1][1] for m in mappings}
            with self.subTest(card=card["name"]):
                self.assertEqual(used, set(card.get("tags", {})))
                self.assertTrue(all(m["card_id"] == 7 for m in mappings))


class LayoutTest(unittest.TestCase):
    def test_tiles_fit_the_24_column_grid_without_overlapping(self):
        taken = set()
        for card in CARDS:
            row, col, width, height = card["layout"]
            with self.subTest(card=card["name"]):
                self.assertLessEqual(col + width, 24)
                cells = {(r, c) for r in range(row, row + height) for c in range(col, col + width)}
                self.assertFalse(cells & taken)
                taken |= cells


CARD_IDS = list(range(101, 101 + len(CARDS)))
BOTTOM = max(c["layout"][0] + c["layout"][3] for c in CARDS)


def cells(d):
    return {(r, c) for r in range(d["row"], d["row"] + d["size_y"]) for c in range(d["col"], d["col"] + d["size_x"])}


class DashboardBodyTest(unittest.TestCase):
    def test_empty_dashboard_gets_every_chart_and_note_with_unique_new_ids(self):
        body = dashboard_body([], CARD_IDS, [])
        ids = [d["id"] for d in body["dashcards"]]
        self.assertEqual(len(ids), 2 * len(CARDS))          # a chart and a one-line note per tile
        self.assertTrue(all(i < 0 for i in ids))
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual({d["card_id"] for d in body["dashcards"]} - {None}, set(CARD_IDS))

    def test_a_second_run_reuses_every_dashcard_id(self):
        first = dashboard_body([], CARD_IDS, [])["dashcards"]
        saved = [dict(d, id=500 + n) for n, d in enumerate(first)]   # what Metabase stores and returns
        again = dashboard_body(saved, CARD_IDS, [])["dashcards"]
        self.assertEqual(sorted(d["id"] for d in again), sorted(d["id"] for d in saved))

    def test_managed_tiles_and_notes_do_not_overlap(self):
        taken = set()
        for d in dashboard_body([], CARD_IDS, [])["dashcards"]:
            self.assertFalse(cells(d) & taken)
            taken |= cells(d)

    def test_hand_added_tiles_move_below_as_a_block_and_keep_their_settings(self):
        text = {"id": 7, "card_id": None, "row": 0, "col": 0, "size_x": 6, "size_y": 2, "parameter_mappings": [],
                "visualization_settings": {"text": "my note", "virtual_card": {"display": "text"}}}
        chart = {"id": 8, "card_id": 999, "row": 2, "col": 0, "size_x": 6, "size_y": 4, "parameter_mappings": [],
                 "visualization_settings": {}}
        out = {d["id"]: d for d in dashboard_body([text, chart], CARD_IDS, [])["dashcards"]}
        self.assertEqual(out[7]["visualization_settings"]["text"], "my note")
        self.assertGreaterEqual(out[7]["row"], BOTTOM)
        self.assertEqual(out[8]["row"] - out[7]["row"], 2)          # same relative layout, no stacking
        self.assertFalse(cells(out[7]) & cells(out[8]))

    def test_a_second_copy_of_a_managed_chart_is_kept(self):
        copy = {"id": 9, "card_id": CARD_IDS[0], "row": 40, "col": 0, "size_x": 12, "size_y": 6,
                "parameter_mappings": [], "visualization_settings": {}}
        first = dict(copy, id=8, row=0)
        ids = {d["id"] for d in dashboard_body([first, copy], CARD_IDS, [])["dashcards"]}
        self.assertTrue({8, 9} <= ids)

    def test_the_note_of_a_renamed_or_removed_chart_is_dropped(self):
        first = dashboard_body([], CARD_IDS, [])["dashcards"]
        saved = [dict(d, id=500 + n) for n, d in enumerate(first)]
        old = dict(saved[0], id=999, visualization_settings=dict(saved[0]["visualization_settings"]))
        old["visualization_settings"][NOTE_KEY] = "A chart that no longer exists"
        ids = {d["id"] for d in dashboard_body(saved + [old], CARD_IDS, [])["dashcards"]}
        self.assertNotIn(999, ids)
        self.assertEqual(ids, {d["id"] for d in saved})

    def test_a_hand_added_filter_survives(self):
        mine = {"id": "my_filter", "name": "Mine", "slug": "mine", "type": "string/="}
        params = dashboard_body([], CARD_IDS, [mine])["parameters"]
        self.assertIn("my_filter", {p["id"] for p in params})
        self.assertTrue({f["id"] for f in FILTERS} <= {p["id"] for p in params})


if __name__ == "__main__":
    unittest.main()
