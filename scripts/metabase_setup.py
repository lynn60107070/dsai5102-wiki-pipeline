"""Set up Metabase through its API: admin account, the `serving` database connection, the charts in
CARDS, and the dashboard that holds them.

Safe to run again: each step checks whether its object already exists (by name) and skips it, and any
chart in CARDS that is missing from the dashboard is added below the existing tiles.
Sumaiya's dashboard (S9) is built on top of the "Wiki serving" connection this creates.

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
    },
    {
        "name": "Edits per hour: bots vs humans",
        "description": "Edits per UTC hour on all Wikimedia wikis, split into bot accounts and humans "
                       "(registered + logged-out). Source: the warehouse (mart_edits_hourly).",
        "display": "line",
        "sql": """\
SELECT hour_ts AS "Hour (UTC)",
       CASE WHEN editor_type = 'bot' THEN 'Bots' ELSE 'Humans' END AS "Editor",
       SUM(edits) AS "Edits"
FROM mart_edits_hourly
GROUP BY 1, 2
ORDER BY 1""",
        "viz": {"graph.dimensions": ["Hour (UTC)", "Editor"], "graph.metrics": ["Edits"]},
    },
]


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
        if card["name"] not in existing_cards:
            response = session.post(f"{BASE}/api/card", timeout=60, json={
                "name": card["name"], "description": card["description"], "display": card["display"],
                "dataset_query": {"type": "native", "database": database_id, "native": {"query": card["sql"]}},
                "visualization_settings": card["viz"],
            })
            response.raise_for_status()
            existing_cards[card["name"]] = response.json()["id"]
            print(f"created the chart '{card['name']}'")
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

    dashcards = session.get(f"{BASE}/api/dashboard/{dashboard_id}", timeout=30).json()["dashcards"]
    on_dashboard = {d["card_id"] for d in dashcards}
    next_row = max((d["row"] + d["size_y"] for d in dashcards), default=0)
    keep = [{k: d[k] for k in ("id", "card_id", "row", "col", "size_x", "size_y")} for d in dashcards]
    new = []
    for card_id in card_ids:
        if card_id not in on_dashboard:
            new.append({"id": -(len(new) + 1), "card_id": card_id, "row": next_row, "col": 0,
                        "size_x": 24, "size_y": 8})
            next_row += 8
    if new:
        session.put(f"{BASE}/api/dashboard/{dashboard_id}", timeout=30,
                    json={"dashcards": keep + new}).raise_for_status()
        print(f"added {len(new)} chart(s) to the dashboard")

    print(f"done: Metabase dashboard id {dashboard_id} ({DASHBOARD_NAME})")


if __name__ == "__main__":
    main()
