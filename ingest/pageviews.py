"""Batch source: one hourly Wikipedia pageview file -> two Parquet files.  (Owner: Sumaiya, task S2)

Source: https://dumps.wikimedia.org/other/pageviews/YYYY/YYYY-MM/pageviews-YYYYMMDD-HH0000.gz
Each line is `domain_code page_title count_views total_response_size`, for example
`en.m Doha 412 0`. One file is ~57 MB compressed, ~6.4M rows (5.25M of them Wikipedia).

Contract (Lynn's Airflow DAG in L5 calls exactly this, so keep the signature):

    fetch_hour(day, hour, out_dir, min_views=10) -> {"articles": Path, "wiki_totals": Path}

It writes both files into out_dir/dt=YYYY-MM-DD/hour=HH/ (create the folders):

articles.parquet: one row per wiki x article x access, only rows with views >= min_views
    hour_ts  timestamp, UTC, start of the hour the views belong to
    wiki     wiki id as in the edit stream: `en` -> `enwiki`, `zh-min-nan` -> `zh_min_nanwiki`
    access   'desktop' (domain code `en`) or 'mobile' (`en.m`)
    article  page title exactly as in the file (underscores, not spaces)
    views    int

wiki_totals.parquet: one row per wiki x access, summed over ALL Wikipedia rows (no threshold)
    hour_ts, wiki, access   as above
    views            int, total views
    articles_viewed  int, number of distinct articles with >= 1 view

Why the threshold and the totals: docs/design.md section 7 (measured on a real file).

Which rows are Wikipedia (docs/design.md section 7):
  - domain code with no suffix (`en`) = desktop, `.m` suffix (`en.m`) = mobile
  - BUT `commons`, `meta`, `species`, `incubator`, `wikidata`, `foundation` are not Wikipedias, and
    `commons.m` means Wikimedia Commons, not mobile. Drop them. Also drop codes like `""`.
  - everything with another suffix (`.d` Wiktionary, `.s` Wikisource, `.m.d`, ...) is dropped

TODO (S2):
  1. Implement fetch_hour following the contract above. Stream the file line by line (it's large), and
     don't load all 6.4M rows into memory at once if you can avoid it.
  2. Check in the Wikimedia docs whether the file named HH0000 holds the hour *starting* or *ending*
     at HH, and set hour_ts accordingly (write the answer in this docstring).
  3. Record for the report: file size, rows before -> after each filter, time taken.
  4. Send a User-Agent header (use WIKI_USER_AGENT from .env), as Wikimedia asks.
Test on one hour: `python -m ingest.pageviews 2026-10-28 12 --out data/pageviews`

Downstream (Lynn, already built and tested on a fixture with this exact contract): dbt models
stg_pageviews / stg_wiki_views -> fact_pageviews_hourly / fact_wiki_views_hourly -> mart_attention_vs_edits.
They switch on with `pageviews_enabled: true` in dbt/dbt_project.yml once real files are in the lake.
"""
import argparse
from datetime import date
from pathlib import Path

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
NOT_WIKIPEDIA = {"commons", "meta", "species", "incubator", "wikidata", "foundation"}


def file_url(day: date, hour: int) -> str:
    return f"{BASE_URL}/{day:%Y}/{day:%Y-%m}/pageviews-{day:%Y%m%d}-{hour:02d}0000.gz"


def fetch_hour(day: date, hour: int, out_dir: Path, min_views: int = 10) -> dict:
    """Download one hourly pageview file and write articles.parquet + wiki_totals.parquet."""
    raise NotImplementedError("S2: implement me (see the module docstring)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("day", type=date.fromisoformat, help="UTC date, e.g. 2026-10-28")
    parser.add_argument("hour", type=int, help="UTC hour 0-23")
    parser.add_argument("--out", type=Path, default=Path("data/pageviews"))
    parser.add_argument("--min-views", type=int, default=10)
    args = parser.parse_args()
    print(fetch_hour(args.day, args.hour, args.out, args.min_views))
