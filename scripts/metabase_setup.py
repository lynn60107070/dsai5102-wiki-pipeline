"""Set up Metabase through its API: admin account, the `serving` database connection, the charts in
CARDS, and the dashboard that holds them.

Safe to run again. CARDS and FILTERS are the source of truth: charts are created or overwritten by name,
placed per their `layout` with a one-line note above each, and wired to the filters. Edit them here, not in
the Metabase UI, because the next run (every `docker compose up`) puts them back. Anything else on the
dashboard (tiles, notes or filters added by hand) is kept; hand-added tiles move below the managed ones.

Runs automatically in the `bootstrap` service on `docker compose up`. To run it by hand on your laptop:
  python scripts/metabase_setup.py
Settings: environment variables first, then .env, then .env.example (repo root).
"""
import os
import time
from pathlib import Path

import requests

BASE = os.environ.get("METABASE_URL", "http://localhost:3000")
SETTINGS = ("METABASE_ADMIN_EMAIL", "METABASE_ADMIN_PASSWORD", "METABASE_DB_PASSWORD")
DATABASE_NAME = "Wiki serving"
DASHBOARD_NAME = "Wikipedia edits"

# Dashboard filters. A card opts in by using the filter's tag in its SQL, always inside [[ ... ]] so the
# card still works when the filter is empty. Dates are inclusive and in UTC; From must not be after To.
FILTERS = [
    {"id": "f_from", "name": "From", "slug": "from", "type": "date/single", "sectionId": "date",
     "tag": "date_from", "tag_type": "date"},
    {"id": "f_to", "name": "To", "slug": "to", "type": "date/single", "sectionId": "date",
     "tag": "date_to", "tag_type": "date"},
    # one value only: the tag is pasted into `= {{project}}`, so a list would be a SQL error
    {"id": "f_project", "name": "Project", "slug": "project", "type": "string/=", "sectionId": "string",
     "tag": "project", "tag_type": "text", "isMultiSelect": False,
     "values_source_type": "static-list", "values_query_type": "list",
     "values_source_config": {"values": ["wikipedia", "wikidata", "commons", "wikisource", "wiktionary",
                                         "meta", "mediawiki", "wikivoyage", "species", "wikiquote", "wikibooks",
                                         "incubator", "wikiversity", "wikifunctions"]}},
]
DATE_FILTER = "[[AND date >= {{date_from}}]] [[AND date <= {{date_to}}]]"
# for tables without a `date` column; pinned to UTC so a report time zone set in Metabase can't shift days
HOUR_FILTER = ("[[AND hour_ts >= {{date_from}}::timestamp AT TIME ZONE 'UTC']] "
               "[[AND hour_ts < ({{date_to}} + 1)::timestamp AT TIME ZONE 'UTC']]")
PROJECT_FILTER = "[[AND project_family = {{project}}]]"
DATES = {"date_from": "date", "date_to": "date"}

# layout: (row, col, width, height) on Metabase's 24-column grid, including the one-row `note` on top.
# `note` is the visible one-line explanation; `description` is the longer text behind the (i) icon.
CARDS = [
    {
        "name": "Live: edits per minute, last 3 hours",
        "description": "Near-real-time: edits per minute on all Wikimedia wikis, bots vs humans, straight "
                       "from the stream processor (Kafka topic edits.per_minute). A minute appears about "
                       "3 minutes after it ends. Add #refresh=60 to the dashboard URL to auto-refresh.",
        "display": "line",
        "sql": """\
SELECT minute_ts AS "Minute (UTC)", SUM(bot_edits) AS "Bots", SUM(human_edits) AS "Humans"
FROM live_edits_per_minute
WHERE minute_ts > now() - interval '3 hours'
GROUP BY 1
ORDER BY 1""",
        "viz": {"graph.dimensions": ["Minute (UTC)"], "graph.metrics": ["Bots", "Humans"]},
        "note": "Edits per minute straight from the stream, about 3 minutes behind real time.",
        "layout": (0, 0, 12, 8),
    },
    {
        "name": "Edits per hour: bots vs humans",
        "description": "Edits per UTC hour, split into bot accounts and humans (registered + logged-out), "
                       "on all Wikimedia wikis or the chosen project. Source: the warehouse (mart_edits_hourly).",
        "display": "line",
        "sql": f"""\
SELECT hour_ts AS "Hour (UTC)",
       CASE WHEN editor_type = 'bot' THEN 'Bots' ELSE 'Humans' END AS "Editor",
       SUM(edits) AS "Edits"
FROM mart_edits_hourly
WHERE TRUE {DATE_FILTER} {PROJECT_FILTER}
GROUP BY 1, 2
ORDER BY 1""",
        # line.missing: leave collector gaps empty instead of drawing a straight line across them
        "viz": {"graph.dimensions": ["Hour (UTC)", "Editor"], "graph.metrics": ["Edits"], "line.missing": "none"},
        "tags": {**DATES, "project": "text"},
        "note": "Bots vs humans per hour; a gap means the collector was off.",
        "layout": (0, 12, 12, 8),
    },
    {
        "name": "Bot share of edits: the 15 busiest wikis",
        "description": "Share of edits made by bot accounts on the 15 wikis with the most edits in the chosen "
                       "dates. Bots are accounts with Wikimedia's self-declared bot flag. Source: mart_wiki_activity.",
        "display": "row",
        "sql": f"""\
SELECT CASE WHEN language_name IS NULL THEN initcap(project_family)
            ELSE language_name || ' ' || initcap(project_family) END AS "Wiki",
       ROUND(100.0 * SUM(bot_edits) / SUM(edits), 1) AS "Bot share (%)",
       SUM(edits) AS "Edits"
FROM mart_wiki_activity
WHERE TRUE {DATE_FILTER} {PROJECT_FILTER}
GROUP BY wiki_id, 1
ORDER BY SUM(edits) DESC
LIMIT 15""",
        "viz": {"graph.dimensions": ["Wiki"], "graph.metrics": ["Bot share (%)"],
                "graph.show_values": True, "graph.y_axis.max": 100,
                "series_settings": {"Bot share (%)": {"color": "#F9D45C"}}},  # bots are yellow on every tile
        "tags": {**DATES, "project": "text"},
        "note": "Share of edits made by flagged bot accounts on the 15 busiest wikis.",
        "layout": (8, 0, 12, 11),
    },
    {
        "name": "Top languages on Wikipedia: bots vs humans",
        "description": "The 15 language editions of Wikipedia with the most edits in the chosen dates, split "
                       "into bot and human edits. Source: mart_wiki_activity (Wikipedias only).",
        "display": "row",
        "sql": f"""\
SELECT language_name AS "Language",
       SUM(edits - bot_edits) AS "Humans",
       SUM(bot_edits) AS "Bots"
FROM mart_wiki_activity
WHERE is_wikipedia {DATE_FILTER}
GROUP BY 1
ORDER BY SUM(edits) DESC
LIMIT 15""",
        "viz": {"graph.dimensions": ["Language"], "graph.metrics": ["Humans", "Bots"],
                "stackable.stack_type": "stacked"},
        "tags": DATES,
        "note": "The Wikipedia languages with the most edits, split into bots and humans.",
        "layout": (8, 12, 12, 11),
    },
    {
        "name": "Wikipedia edits by hour of day: bots vs humans",
        "description": "Average edits in each UTC hour of the day on Wikipedias, for bots and for humans "
                       "(registered and logged-out), over the chosen dates. Humans follow the clock; bots "
                       "much less. Source: mart_edits_hourly.",
        "display": "line",
        "sql": f"""\
SELECT hour_of_day AS "Hour of day (UTC)",
       CASE WHEN is_human THEN 'Humans' ELSE 'Bots' END AS "Editor",
       ROUND(SUM(edits)::numeric / COUNT(DISTINCT date)) AS "Avg edits in this hour"
FROM mart_edits_hourly
WHERE is_wikipedia {DATE_FILTER}
GROUP BY 1, 2
ORDER BY 1""",
        "viz": {"graph.dimensions": ["Hour of day (UTC)", "Editor"], "graph.metrics": ["Avg edits in this hour"],
                "graph.x_axis.scale": "ordinal",
                "series_settings": {"Bots": {"color": "#F9D45C"}, "Humans": {"color": "#A989C5"}}},
        "tags": DATES,
        "note": "Average Wikipedia edits per UTC hour: humans peak mid-afternoon.",
        "layout": (19, 0, 12, 10),
    },
    {
        "name": "Do attention spikes bring edits? Spikes vs steadily popular articles",
        "description": "Share of Wikipedia article-hours with at least one edit in the 2 hours after, by "
                       "views in the hour. Spikes are hours with at least 5 times the article's average "
                       "views of the previous 24 hours; the others have as many views but no jump. Main "
                       "pages and non-article pages (Special:, Wikipedia:, ...) are left out: they get huge "
                       "views and almost no edits. Source: mart_attention_vs_edits (>= 1,000 views).",
        "display": "bar",
        "sql": f"""\
SELECT CASE WHEN views >= 20000 THEN '3. 20,000+ views'
            WHEN views >= 5000 THEN '2. 5,000-20,000'
            ELSE '1. 1,000-5,000' END AS "Views in the hour",
       CASE WHEN is_spike THEN 'Spikes' ELSE 'Steadily popular' END AS "Article-hours",
       ROUND(100.0 * AVG(CASE WHEN edits_next_2h > 0 THEN 1 ELSE 0 END), 1) AS "% with an edit in the next 2 h"
FROM mart_attention_vs_edits
WHERE has_full_baseline AND is_content_page {HOUR_FILTER}
GROUP BY 1, 2
ORDER BY 1, 2""",
        "viz": {"graph.dimensions": ["Views in the hour", "Article-hours"],
                "graph.metrics": ["% with an edit in the next 2 h"], "graph.show_values": True,
                "series_settings": {"Spikes": {"color": "#EF8C8C"}, "Steadily popular": {"color": "#A7ADB8"}}},
        "tags": DATES,
        "note": "Share followed by an edit within 2 h: attention spikes against steadily popular articles.",
        "layout": (19, 12, 12, 10),
    },
    {
        "name": "Reader attention spikes and the edits that follow",
        "description": "Wikipedia articles whose views in an hour jumped to at least 5 times their average of "
                       "the previous 24 hours (and at least 1,000 views), with the edits in that hour and the "
                       "2 hours after (articles only). Source: mart_attention_vs_edits. Empty until 24 hours of pageviews exist.",
        "display": "table",
        "sql": f"""\
SELECT hour_ts AS "Hour (UTC)",
       COALESCE((SELECT max(w.language_name) FROM mart_wiki_activity w WHERE w.wiki_id = a.wiki_id),
                a.wiki_id) || ' Wikipedia' AS "Wiki",
       replace(title, '_', ' ') AS "Article",
       views AS "Views",
       -- no views in the previous 24 hours: a jump from nothing, so there is no ratio to show
       COALESCE(ROUND(views / NULLIF(prev_24h_avg_views, 0))::int || '×', 'new') AS "Times usual",
       edits_same_hour AS "Edits, same hour",
       edits_next_2h AS "Edits, next 2 hours"
FROM mart_attention_vs_edits a
WHERE is_spike AND is_content_page {HOUR_FILTER}
ORDER BY views DESC
LIMIT 25""",
        "viz": {},
        "tags": DATES,
        "note": "Articles whose hourly views jumped to at least 5× their usual level, and the edits that followed.",
        "layout": (29, 0, 24, 10),
    },
    {
        "name": "Data completeness: edits collected per hour",
        "description": "Edits stored per UTC hour. Hours with no bar are gaps, when the collector was offline "
                       "(in our run, none from 28 Oct to 11 Nov); partial hours (orange) have under half the usual volume "
                       "and are kept but marked. "
                       "Source: mart_data_completeness.",
        "display": "bar",
        "sql": f"""\
SELECT hour_ts AS "Hour (UTC)",
       CASE WHEN is_partial THEN 'Partial' ELSE 'Complete' END AS "Status",
       edits AS "Edits"
FROM mart_data_completeness
WHERE NOT is_gap {DATE_FILTER}
ORDER BY 1""",
        "viz": {"graph.dimensions": ["Hour (UTC)", "Status"], "graph.metrics": ["Edits"],
                "stackable.stack_type": "stacked",
                "series_settings": {"Complete": {"color": "#509EE3"}, "Partial": {"color": "#F2A86F"}}},
        "tags": DATES,
        "note": "Edits stored per hour; orange hours have under half the usual volume.",
        "layout": (39, 0, 24, 8),
    },
]


def template_tags(card: dict) -> dict:
    """The card's filter variables, in the shape Metabase's native queries expect."""
    names = {f["tag"]: f["name"] for f in FILTERS}
    return {tag: {"id": tag, "name": tag, "display-name": names[tag], "type": kind, "required": False}
            for tag, kind in card.get("tags", {}).items()}


def parameter_mappings(card: dict, card_id: int) -> list:
    """Wires each dashboard filter to the card's variable of the same tag (only the ones the card uses)."""
    return [{"parameter_id": f["id"], "card_id": card_id, "target": ["variable", ["template-tag", f["tag"]]]}
            for f in FILTERS if f["tag"] in card.get("tags", {})]


NOTE_KEY = "metabase_setup_note"   # marks the note tiles this script owns, so a re-run finds them again
KEEP = ("id", "card_id", "row", "col", "size_x", "size_y", "parameter_mappings", "visualization_settings",
        "series", "dashboard_tab_id")


def dashboard_body(dashcards: list, card_ids: list, parameters: list) -> dict:
    """The PUT body for the dashboard: each CARDS chart (card_ids, in the same order) with its note above it,
    then every other tile unchanged except moved, as a block, below them; FILTERS plus any other filters.
    Metabase deletes any tile missing from the body, so nothing else may be left out."""
    new_id = iter(range(-1, -1000, -1))
    charts = {}                                      # card_id -> the first tile showing it
    notes = {}
    for d in dashcards:
        note = (d.get("visualization_settings") or {}).get(NOTE_KEY)
        if note:
            notes.setdefault(note, d)
        elif d.get("card_id") is not None:
            charts.setdefault(d["card_id"], d)
    managed = []
    for card, card_id in zip(CARDS, card_ids):
        row, col, size_x, size_y = card["layout"]
        managed.append({"id": notes[card["name"]]["id"] if card["name"] in notes else next(new_id),
                        "card_id": None, "row": row, "col": col, "size_x": size_x, "size_y": 1,
                        "parameter_mappings": [],
                        "visualization_settings": {"virtual_card": {"name": None, "display": "text",
                                                                    "visualization_settings": {},
                                                                    "dataset_query": {}, "archived": False},
                                                   "text": card["note"], "dashcard.background": False,
                                                   "text.align_vertical": "bottom", NOTE_KEY: card["name"]}})
        managed.append({"id": charts[card_id]["id"] if card_id in charts else next(new_id),
                        "card_id": card_id, "row": row + 1, "col": col, "size_x": size_x, "size_y": size_y - 1,
                        "parameter_mappings": parameter_mappings(card, card_id)})
    reused = {d["id"] for d in managed}
    # A note whose chart is no longer in CARDS (renamed or removed) is dropped, not kept as an orphan.
    stale = {d["id"] for name, d in notes.items() if name not in {c["name"] for c in CARDS}}
    others = [{k: d[k] for k in KEEP if k in d} for d in dashcards if d["id"] not in reused | stale]
    bottom = max(c["layout"][0] + c["layout"][3] for c in CARDS)
    shift = max(0, bottom - min((d["row"] for d in others), default=bottom))
    for d in others:
        d["row"] += shift
    ours = [{k: v for k, v in f.items() if k not in ("tag", "tag_type")} for f in FILTERS]
    theirs = [p for p in parameters if p["id"] not in {f["id"] for f in FILTERS}]
    return {"parameters": ours + theirs, "dashcards": managed + others}


def load_env() -> dict:
    values = {}
    root = Path(__file__).resolve().parent.parent
    for name in (".env.example", ".env"):
        path = root / name
        if path.exists():
            for line in path.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    values[key.strip()] = value.strip()
    values.update({k: os.environ[k] for k in SETTINGS if k in os.environ})
    missing = [k for k in SETTINGS if k not in values]
    if missing:
        raise SystemExit(f"missing settings: {', '.join(missing)}")
    return values


def wait_until_healthy():
    for _ in range(60):
        try:
            if requests.get(f"{BASE}/api/health", timeout=5).json().get("status") == "ok":
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise SystemExit(f"Metabase is not up on {BASE} (is the stack running?)")


def as_list(response):
    data = response.json()
    return data.get("data", data) if isinstance(data, dict) else data


def main():
    env = load_env()
    wait_until_healthy()

    props = requests.get(f"{BASE}/api/session/properties", timeout=30).json()
    if not props.get("has-user-setup"):
        requests.post(f"{BASE}/api/setup", timeout=60, json={
            "token": props["setup-token"],
            "user": {"email": env["METABASE_ADMIN_EMAIL"], "password": env["METABASE_ADMIN_PASSWORD"],
                     "first_name": "Wiki", "last_name": "Admin"},
            "prefs": {"site_name": "Wikipedia live edits", "site_locale": "en", "allow_tracking": False},
        }).raise_for_status()
        print("created the Metabase admin account")

    session = requests.Session()
    login = session.post(f"{BASE}/api/session", timeout=30, json={
        "username": env["METABASE_ADMIN_EMAIL"], "password": env["METABASE_ADMIN_PASSWORD"]})
    login.raise_for_status()
    session.headers["X-Metabase-Session"] = login.json()["id"]

    database_id = next((d["id"] for d in as_list(session.get(f"{BASE}/api/database", timeout=30))
                        if d["name"] == DATABASE_NAME), None)
    if database_id is None:
        response = session.post(f"{BASE}/api/database", timeout=120, json={
            "engine": "postgres", "name": DATABASE_NAME,
            "details": {"host": "postgres", "port": 5432, "dbname": "serving", "user": "metabase_ro",
                        "password": env["METABASE_DB_PASSWORD"], "ssl": False},
        })
        response.raise_for_status()
        database_id = response.json()["id"]
        print(f"connected Metabase to Postgres `serving` as the read-only user (database id {database_id})")
    session.post(f"{BASE}/api/database/{database_id}/sync_schema", timeout=60).raise_for_status()

    existing_cards = {c["name"]: c["id"] for c in as_list(session.get(f"{BASE}/api/card", timeout=30))}
    card_ids = []
    for card in CARDS:
        body = {
            "name": card["name"], "description": card["description"], "display": card["display"],
            "dataset_query": {"type": "native", "database": database_id,
                              "native": {"query": card["sql"], "template-tags": template_tags(card)}},
            "visualization_settings": card["viz"],
        }
        if card["name"] not in existing_cards:
            response = session.post(f"{BASE}/api/card", timeout=60, json=body)
            response.raise_for_status()
            existing_cards[card["name"]] = response.json()["id"]
            print(f"created the chart '{card['name']}'")
        else:  # keep existing charts in step with CARDS (SQL, filters, chart settings)
            session.put(f"{BASE}/api/card/{existing_cards[card['name']]}", timeout=60, json=body).raise_for_status()
        card_ids.append(existing_cards[card["name"]])

    dashboard_id = next((d["id"] for d in as_list(session.get(f"{BASE}/api/dashboard", timeout=30))
                         if d["name"] == DASHBOARD_NAME), None)
    if dashboard_id is None:
        response = session.post(f"{BASE}/api/dashboard", timeout=30, json={
            "name": DASHBOARD_NAME,
            "description": "Who edits Wikipedia, and when. Data: Wikimedia EventStreams + pageview dumps (UTC)."})
        response.raise_for_status()
        dashboard_id = response.json()["id"]
        print(f"created the dashboard '{DASHBOARD_NAME}'")

    # Charts from CARDS go where their layout says, wired to the filters; any other chart someone added
    # by hand stays where it is, below them.
    dashboard = session.get(f"{BASE}/api/dashboard/{dashboard_id}", timeout=30).json()
    session.put(f"{BASE}/api/dashboard/{dashboard_id}", timeout=30,
                json=dashboard_body(dashboard["dashcards"], card_ids, dashboard.get("parameters") or [])
                ).raise_for_status()
    print(f"dashboard has {len(CARDS)} chart(s) from CARDS, each with a note, and {len(FILTERS)} filters")

    print(f"done: Metabase dashboard id {dashboard_id} ({DASHBOARD_NAME})")


if __name__ == "__main__":
    main()
