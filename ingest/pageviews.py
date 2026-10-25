"""Batch source: hourly Wikipedia pageview files -> one Parquet file per hour.  (Owner: Sumaiya, task S2)

Source: https://dumps.wikimedia.org/other/pageviews/YYYY/YYYY-MM/pageviews-YYYYMMDD-HH0000.gz
Each line is `domain_code page_title count_views total_response_size`, for example
`en.m Doha 412 0`. One file is ~60 MB compressed.

Contract (Lynn's Airflow DAG in L5 calls exactly this, so keep the signature):

    fetch_hour(day, hour, out_dir) -> Path of the Parquet file written

Output columns (one row per wiki x article x access type):
    hour_ts  timestamp, UTC, start of the hour the views belong to
    wiki     wiki id as in the edit stream: `en` -> `enwiki`, `zh-min-nan` -> `zh_min_nanwiki`
    access   'desktop' (domain code `en`) or 'mobile' (`en.m`)
    article  page title exactly as in the file (underscores, not spaces)
    views    int

TODO (S2):
  1. Keep Wikipedia rows only: domain codes with no suffix (desktop) or `.m` (mobile). Drop
     Wiktionary (`.d`), Wikibooks (`.b`), etc. Check the docs for the full list of suffixes.
  2. Check in the Wikimedia docs whether the file named HH0000 holds the hour *starting* or *ending*
     at HH, and set hour_ts accordingly (write the answer in the docstring).
  3. Record for the report: file size, rows before -> after the filter, time taken.
  4. Send a User-Agent header (use WIKI_USER_AGENT from .env), as Wikimedia asks.
Test on one hour: `python -m ingest.pageviews 2026-10-28 12 --out data/pageviews`
"""
import argparse
from datetime import date
from pathlib import Path

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"


def file_url(day: date, hour: int) -> str:
    return f"{BASE_URL}/{day:%Y}/{day:%Y-%m}/pageviews-{day:%Y%m%d}-{hour:02d}0000.gz"


def fetch_hour(day: date, hour: int, out_dir: Path) -> Path:
    """Download one hourly pageview file, keep Wikipedia rows, write Parquet, return its path."""
    raise NotImplementedError("S2: implement me (see the module docstring)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("day", type=date.fromisoformat, help="UTC date, e.g. 2026-10-28")
    parser.add_argument("hour", type=int, help="UTC hour 0-23")
    parser.add_argument("--out", type=Path, default=Path("data/pageviews"))
    args = parser.parse_args()
    print(fetch_hour(args.day, args.hour, args.out))
