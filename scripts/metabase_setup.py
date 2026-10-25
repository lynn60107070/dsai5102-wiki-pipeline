"""Set up Metabase through its API: admin account, the `serving` database connection, and the first chart.

Safe to run again: each step checks whether its object already exists (by name) and skips it.
Sumaiya's dashboard (S9) is built on top of the "Wiki serving" connection this creates.

Run on your laptop, with the stack up:  python scripts/metabase_setup.py
(reads .env from the repo root; Metabase on http://localhost:3000)
"""
import time
from pathlib import Path

import requests

BASE = "http://localhost:3000"
DATABASE_NAME = "Wiki serving"
DASHBOARD_NAME = "Wikipedia edits"
CARD_NAME = "Edits per hour: bots vs humans"
CARD_SQL = """\
SELECT hour_ts AS "Hour (UTC)",
       CASE WHEN editor_type = 'bot' THEN 'Bots' ELSE 'Humans' END AS "Editor",
       SUM(edits) AS "Edits"
FROM mart_edits_hourly
GROUP BY 1, 2
ORDER BY 1"""


def load_env() -> dict:
    env_file = Path(__file__).resolve().parent.parent / ".env"
    pairs = (line.strip().split("=", 1) for line in env_file.read_text().splitlines())
    return {k: v for k, *rest in pairs if rest and not k.startswith("#") for v in rest}


def wait_until_healthy():
    for _ in range(60):
        try:
            if requests.get(f"{BASE}/api/health", timeout=5).json().get("status") == "ok":
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise SystemExit("Metabase is not up on http://localhost:3000 (is the stack running?)")


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

    databases = session.get(f"{BASE}/api/database", timeout=30).json()
    databases = databases.get("data", databases) if isinstance(databases, dict) else databases
    database_id = next((d["id"] for d in databases if d["name"] == DATABASE_NAME), None)
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

    cards = session.get(f"{BASE}/api/card", timeout=30).json()
    card_id = next((c["id"] for c in cards if c["name"] == CARD_NAME), None)
    if card_id is None:
        response = session.post(f"{BASE}/api/card", timeout=60, json={
            "name": CARD_NAME,
            "description": "Edits per UTC hour on all Wikimedia wikis, split into bot accounts and humans "
                           "(registered + logged-out). Source: Wikimedia EventStreams.",
            "display": "line",
            "dataset_query": {"type": "native", "database": database_id, "native": {"query": CARD_SQL}},
            "visualization_settings": {"graph.dimensions": ["Hour (UTC)", "Editor"], "graph.metrics": ["Edits"]},
        })
        response.raise_for_status()
        card_id = response.json()["id"]
        print(f"created the chart '{CARD_NAME}'")

    dashboards = session.get(f"{BASE}/api/dashboard", timeout=30).json()
    dashboard_id = next((d["id"] for d in dashboards if d["name"] == DASHBOARD_NAME), None)
    if dashboard_id is None:
        response = session.post(f"{BASE}/api/dashboard", timeout=30, json={
            "name": DASHBOARD_NAME,
            "description": "Who edits Wikipedia, and when. Data: Wikimedia EventStreams + pageview dumps (UTC)."})
        response.raise_for_status()
        dashboard_id = response.json()["id"]
        session.put(f"{BASE}/api/dashboard/{dashboard_id}", timeout=30, json={"dashcards": [
            {"id": -1, "card_id": card_id, "row": 0, "col": 0, "size_x": 24, "size_y": 8}]}).raise_for_status()
        print(f"created the dashboard '{DASHBOARD_NAME}'")

    print(f"done: {BASE}/dashboard/{dashboard_id}")


if __name__ == "__main__":
    main()
