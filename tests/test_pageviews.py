import unittest
from datetime import date

from ingest.pageviews import file_url, parse_line


class ParseLineTest(unittest.TestCase):
    def test_wikipedia_desktop_and_mobile(self):
        self.assertEqual(parse_line("en Doha 412 0\n"), ("enwiki", "desktop", "Doha", 412))
        self.assertEqual(parse_line("ar.m الدوحة 77 0\n"), ("arwiki", "mobile", "الدوحة", 77))

    def test_wiki_ids_follow_the_edit_stream(self):
        self.assertEqual(parse_line("zh-min-nan Tâi-oân 3 0")[0], "zh_min_nanwiki")
        self.assertEqual(parse_line("be-tarask Мінск 5 0")[0], "be_x_oldwiki")

    def test_other_projects_and_bad_lines_are_dropped(self):
        for line in ["commons.m File:X.jpg 9 0",   # Wikimedia Commons, not mobile Wikipedia
                     "commons Main_Page 9 0",
                     "wikidata Q42 9 0",
                     "en.d dictionary 9 0",        # Wiktionary
                     "en.m.d dictionary 9 0",      # mobile Wiktionary
                     '"" Category:Contents 2 0',
                     "en Doha notanumber 0",
                     "en Doha 412"]:
            self.assertIsNone(parse_line(line), line)


class FileUrlTest(unittest.TestCase):
    def test_url(self):
        self.assertEqual(file_url(date(2026, 10, 24), 12),
                         "https://dumps.wikimedia.org/other/pageviews/2026/2026-10/pageviews-20261024-120000.gz")


if __name__ == "__main__":
    unittest.main()
