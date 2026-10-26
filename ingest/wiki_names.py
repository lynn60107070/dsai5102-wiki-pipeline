"""Reference data: every Wikimedia wiki with its language -> dbt/seeds/dim_wiki_names.csv  (Task S3)

Source: the sitematrix API (https://meta.wikimedia.org/w/api.php?action=sitematrix), which lists every
wiki by database name (`arwiki`, the same id as `wiki` in the edit stream), its language code, the
language's name in English and in the language itself, and its project. The dbt model dim_wiki joins
this on wiki_id; wikis missing here still get project and language code from their server name.

The list changes rarely, so this runs by hand and the CSV is committed:
    python -m ingest.wiki_names
"""
import csv
import os
from pathlib import Path

import requests

API = ("https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json&uselang=en"
       "&smtype=language|special&smlangprop=code|name|localname|site"
       "&smsiteprop=url|dbname|code|sitename&smlimit=max")
USER_AGENT = os.environ.get(
    "WIKI_USER_AGENT",
    "dsai5102-wiki-pipeline/0.1 (https://github.com/lynn60107070/dsai5102-wiki-pipeline; student project)")
OUT = Path(__file__).resolve().parent.parent / "dbt" / "seeds" / "dim_wiki_names.csv"
COLUMNS = ["wiki_id", "language_code", "language_name", "local_language_name", "project_family",
           "site_url", "is_arabic"]
# Arabic and its varieties with their own Wikipedia (Egyptian, Moroccan): the "local" lens of the report.
ARABIC = {"ar", "arz", "ary"}
# sitematrix project codes -> the project_family values used in dim_wiki
FAMILY = {"wiki": "wikipedia"}


def rows(matrix: dict):
    for key, language in matrix.items():
        if not key.isdigit():
            continue
        for site in language.get("site", []):
            yield {
                "wiki_id": site["dbname"],
                "language_code": language["code"],
                "language_name": language.get("localname") or language.get("name"),
                "local_language_name": language.get("name"),
                "project_family": FAMILY.get(site["code"], site["code"]),
                "site_url": site["url"],
                "is_arabic": language["code"] in ARABIC,
            }
    for site in matrix.get("specials", []):  # Commons, Meta, Wikidata, ...: no language of their own
        yield {
            "wiki_id": site["dbname"], "language_code": None, "language_name": None,
            "local_language_name": None, "project_family": site["code"], "site_url": site["url"],
            "is_arabic": False,
        }


def main():
    response = requests.get(API, headers={"User-Agent": USER_AGENT}, timeout=60)
    response.raise_for_status()
    records = sorted(rows(response.json()["sitematrix"]), key=lambda r: r["wiki_id"])
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(records)
    print(f"wrote {len(records)} wikis to {OUT.relative_to(OUT.parent.parent.parent)}")


if __name__ == "__main__":
    main()
