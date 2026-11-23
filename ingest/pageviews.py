"""Batch source: one hourly Wikipedia pageview file -> two Parquet files.

Source: https://dumps.wikimedia.org/other/pageviews/YYYY/YYYY-MM/pageviews-YYYYMMDD-HH0000.gz (CC0)
Each line is `domain_code page_title count_views total_response_size`, for example `en.m Doha 412 0`.
One file is ~57 MB compressed, ~6.4M rows (5.25M of them Wikipedia).

**Which hour a file holds** (verified 26 Oct against the official Pageview API): the file named
`...-HH0000.gz` holds the hour *ending* at HH. pageviews-20261024-120000.gz has exactly the API's
11:00-12:00 numbers for English Wikipedia (desktop 2,077,400; desktop + mobile 6,708,289). The dumps
count user traffic only (they match the API's `user` agent, not `all-agents`), and `.m` is mobile web
plus the mobile apps.

Contract (the Airflow DAG `pageviews_hourly` calls exactly this):

    fetch_hour(day, hour, out_dir, min_views=10) -> {"articles": Path, "wiki_totals": Path}

`day`/`hour` are the date and hour in the FILE NAME; the views belong to the hour before. Both files go
to out_dir/dt=YYYY-MM-DD/hour=HH/ for the hour the views belong to (hour_ts):

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

Why the threshold and the totals: docs/design.md section 7. Test one file:
`python -m ingest.pageviews 2026-10-24 12 --out data/pageviews`
"""
import argparse
import gzip
import io
import logging
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
USER_AGENT = os.environ.get(
    "WIKI_USER_AGENT",
    "dsai5102-wiki-pipeline/0.1 (https://github.com/lynn60107070/dsai5102-wiki-pipeline; student project)")

# Projects whose domain code has no suffix but which are not Wikipedias.
NOT_WIKIPEDIA = {"commons", "meta", "species", "incubator", "wikidata", "foundation", "mediawiki",
                 "wikifunctions", "wikitech", "outreach", "login", "sources", "wikimania"}
# Wikipedias whose database name doesn't follow the `<code>wiki` rule.
WIKI_ID_EXCEPTIONS = {"be-tarask": "be_x_oldwiki"}
LANGUAGE_CODE = re.compile(r"^[a-z][a-z0-9-]*$")

ARTICLES_SCHEMA = pa.schema([("hour_ts", pa.timestamp("s", tz="UTC")), ("wiki", pa.string()),
                             ("access", pa.string()), ("article", pa.string()), ("views", pa.int64())])
TOTALS_SCHEMA = pa.schema([("hour_ts", pa.timestamp("s", tz="UTC")), ("wiki", pa.string()),
                           ("access", pa.string()), ("views", pa.int64()), ("articles_viewed", pa.int64())])

log = logging.getLogger("pageviews")


def file_url(day: date, hour: int) -> str:
    return f"{BASE_URL}/{day:%Y}/{day:%Y-%m}/pageviews-{day:%Y%m%d}-{hour:02d}0000.gz"


def parse_line(line: str):
    """(wiki, access, article, views) for a Wikipedia row, or None for any other project or bad line."""
    parts = line.rstrip("\n").split(" ")
    if len(parts) != 4:
        return None
    code, article, views = parts[0], parts[1], parts[2]
    segments = code.split(".")
    if len(segments) == 1:
        access = "desktop"
    elif len(segments) == 2 and segments[1] == "m":
        access = "mobile"
    else:
        return None  # another project (.d Wiktionary, .s Wikisource, .m.d, ...)
    language = segments[0]
    if language in NOT_WIKIPEDIA or not LANGUAGE_CODE.match(language) or not views.isdigit():
        return None
    wiki = WIKI_ID_EXCEPTIONS.get(language, language.replace("-", "_") + "wiki")
    return wiki, access, article, int(views)


def fetch_hour(day: date, hour: int, out_dir: Path, min_views: int = 10) -> dict:
    """Download one hourly pageview file and write articles.parquet + wiki_totals.parquet."""
    started = time.monotonic()
    hour_ts = datetime(day.year, day.month, day.day, hour, tzinfo=timezone.utc) - timedelta(hours=1)
    url = file_url(day, hour)
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, stream=True, timeout=(10, 120))
    response.raise_for_status()

    articles = {name: [] for name in ARTICLES_SCHEMA.names}
    totals = defaultdict(lambda: [0, 0])  # (wiki, access) -> [views, articles_viewed]
    rows_read = rows_wikipedia = 0
    with gzip.GzipFile(fileobj=response.raw) as compressed:
        for line in io.TextIOWrapper(compressed, encoding="utf-8", errors="replace"):
            rows_read += 1
            parsed = parse_line(line)
            if parsed is None:
                continue
            rows_wikipedia += 1
            wiki, access, article, views = parsed
            total = totals[(wiki, access)]
            total[0] += views
            total[1] += 1
            if views >= min_views:
                for name, value in zip(("wiki", "access", "article", "views"), parsed):
                    articles[name].append(value)

    articles["hour_ts"] = [hour_ts] * len(articles["views"])
    target = Path(out_dir) / f"dt={hour_ts:%Y-%m-%d}" / f"hour={hour_ts:%H}"
    target.mkdir(parents=True, exist_ok=True)
    paths = {"articles": target / "articles.parquet", "wiki_totals": target / "wiki_totals.parquet"}
    pq.write_table(pa.table(articles, schema=ARTICLES_SCHEMA), paths["articles"], compression="zstd")
    keys = list(totals)
    pq.write_table(pa.table({
        "hour_ts": [hour_ts] * len(keys),
        "wiki": [k[0] for k in keys],
        "access": [k[1] for k in keys],
        "views": [totals[k][0] for k in keys],
        "articles_viewed": [totals[k][1] for k in keys],
    }, schema=TOTALS_SCHEMA), paths["wiki_totals"], compression="zstd")

    log.info("%s -> hour %s: %d rows read, %d Wikipedia rows, %d kept (>= %d views), %d wiki totals, "
             "%.1f MB compressed, %.0f s", url.rsplit("/", 1)[1], f"{hour_ts:%Y-%m-%d %H}:00", rows_read,
             rows_wikipedia, len(articles["views"]), min_views, len(keys),
             int(response.headers.get("Content-Length", 0)) / 1e6, time.monotonic() - started)
    return paths


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("day", type=date.fromisoformat, help="UTC date in the file name, e.g. 2026-10-24")
    parser.add_argument("hour", type=int, help="UTC hour in the file name, 0-23 (views are for the hour before)")
    parser.add_argument("--out", type=Path, default=Path("data/pageviews"))
    parser.add_argument("--min-views", type=int, default=10)
    args = parser.parse_args()
    print(fetch_hour(args.day, args.hour, args.out, args.min_views))
