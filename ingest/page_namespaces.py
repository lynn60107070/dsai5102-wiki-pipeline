"""Reference data: which page titles are not articles -> dbt/seeds/wiki_non_article_titles.csv

The pageview dumps count every page, not just articles: main pages, Special:Search, Wikipedia: project
pages, files, categories... These get huge view counts and are almost never edited, so mixing them in
makes any "views vs edits" comparison meaningless (finding 5, report section 6). For every Wikipedia in
dim_wiki_names.csv this asks the wiki's own siteinfo API
(https://<site>/w/api.php?action=query&meta=siteinfo&siprop=general|namespaces|namespacealiases) for

    kind = 'namespace'  every namespace name except the article namespace (0), local, canonical and
                        aliases: a title starting with '<name>:' is not an article
    kind = 'main_page'  the wiki's main page title (on most wikis it is in the article namespace)

The dbt model mart_attention_vs_edits uses it for the column is_content_page. The lists change rarely,
so this runs by hand (about 3 minutes) and the CSV is committed:
    python -m ingest.page_namespaces
"""
import csv
import os
import time
from pathlib import Path

import requests

SEEDS = Path(__file__).resolve().parent.parent / "dbt" / "seeds"
WIKIS = SEEDS / "dim_wiki_names.csv"
OUT = SEEDS / "wiki_non_article_titles.csv"
QUERY = "/w/api.php?action=query&meta=siteinfo&siprop=general|namespaces|namespacealiases&format=json"
USER_AGENT = os.environ.get(
    "WIKI_USER_AGENT",
    "dsai5102-wiki-pipeline/0.1 (https://github.com/lynn60107070/dsai5102-wiki-pipeline; student project)")


def titles(wiki_id: str, info: dict):
    """(wiki_id, kind, name) rows from one siteinfo answer."""
    names = set()
    for ns in info["namespaces"].values():
        if ns["id"] != 0:
            names.update(n for n in (ns.get("*"), ns.get("canonical")) if n)
    names.update(a["*"] for a in info.get("namespacealiases", []) if a["id"] != 0)
    for name in sorted(names):
        yield {"wiki_id": wiki_id, "kind": "namespace", "name": name}
    yield {"wiki_id": wiki_id, "kind": "main_page", "name": info["general"]["mainpage"]}


def main():
    with open(WIKIS, encoding="utf-8") as f:
        wikis = [(r["wiki_id"], r["site_url"]) for r in csv.DictReader(f) if r["project_family"] == "wikipedia"]
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    rows, failed = [], []
    for wiki_id, site in wikis:
        try:
            response = session.get(site + QUERY, timeout=30)
            response.raise_for_status()
            rows.extend(titles(wiki_id, response.json()["query"]))
        except (requests.RequestException, KeyError, ValueError) as e:  # closed or renamed wikis
            failed.append(f"{wiki_id} ({type(e).__name__})")
        time.sleep(0.2)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["wiki_id", "kind", "name"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} titles for {len(wikis) - len(failed)} of {len(wikis)} Wikipedias to "
          f"{OUT.relative_to(OUT.parent.parent.parent)}")
    if failed:
        print("no answer from:", ", ".join(failed))


if __name__ == "__main__":
    main()
