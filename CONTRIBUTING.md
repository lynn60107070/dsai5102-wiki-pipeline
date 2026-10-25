# Team guide: start here

The stream collector already works: Kafka, the producer (Wikipedia's live edits → `wiki.raw`) and the
raw sink (→ hourly files). You build your own part on top of it.
Tasks and deadlines: **[SCHEDULE.md](SCHEDULE.md)**. Everything we hand in: **[DELIVERABLES.md](DELIVERABLES.md)**.
Design decisions: **[docs/design.md](docs/design.md)**.

## 1. One-time setup (macOS or Windows)

The project runs the same on both: everything heavy runs in Docker, and every image we use has both
Intel/AMD (`amd64`) and Apple Silicon (`arm64`) builds. Only the local Python commands differ.

1. **Docker Desktop** → Settings → Resources: give it **at least 8 GB of memory**. Also tick
   *Start Docker Desktop when you sign in*.
2. Clone and configure:

   **macOS** (Terminal)
   ```bash
   git clone https://github.com/lynn60107070/dsai5102-wiki-pipeline.git
   cd dsai5102-wiki-pipeline
   cp .env.example .env
   ```
   **Windows** (cmd)
   ```bat
   git clone https://github.com/lynn60107070/dsai5102-wiki-pipeline.git
   cd dsai5102-wiki-pipeline
   copy .env.example .env
   ```
3. Python 3.12 for scripts and tests on your laptop (on a Mac without it: `brew install python@3.12`):

   **macOS**
   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python -m unittest -v
   ```
   **Windows**
   ```bat
   py -3.12 -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   python -m unittest -v
   ```
   All tests must pass (offline, no Docker needed).

## 2. Run the collector

Same commands on both systems:

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f producer
```

Within a minute the producer logs about `950 sent to wiki.raw` per minute. The raw sink writes its
first files after 5 minutes. Check them with:

```bash
docker compose exec raw-sink python -m stream.status
```

**Only Lynn's laptop runs the collector 24/7** (that's where the project's data comes from). On your laptop,
run it briefly to test, then `docker compose down` (add `-v` to delete your test data too).

## 3. Who owns what

You edit only the files you own. To change someone else's file, ask them or open a PR they review.

| Owner | Files | Job |
|---|---|---|
| **Lynn** (lead) | `docker-compose.yml`, `stream/`, `airflow/`, `dbt/models/`, `tests/`, `README.md`, `docs/design.md`, `paper/main.tex` + `paper/sections/03`–`05` (+ the Overleaf project) | Architecture, Kafka, lake, Airflow, dbt star schema, integration, paper assembly |
| **Sumaiya** | `ingest/`, `dbt/seeds/`, `docs/diagram/`, `metabase/`, `paper/sections/01`, `02`, `06`, `07` | Pageview ingestion, reference data, diagram, dashboard, insights |
| **Both** | `SCHEDULE.md`, `DELIVERABLES.md`, `CONTRIBUTING.md`, `paper/refs.bib` (add only) | Change only when both agree |

**The paper** is written the same way as the code (branches + PRs, one file per section); see
[paper/README.md](paper/README.md).

## 4. Where your first tasks start

| Task | Start here |
|---|---|
| S2 pageview script | [ingest/pageviews.py](ingest/pageviews.py): the signature, output columns and TODOs are written; the DAG [airflow/dags/pageviews_hourly.py](airflow/dags/pageviews_hourly.py) calls `fetch_hour` exactly as defined. Test it inside Airflow (which has pyarrow 22, so don't rely on newer features): `docker compose exec airflow-scheduler airflow tasks test pageviews_hourly fetch 2026-10-28T12:00:00+00:00` |
| S3 `dim_wiki` seed | [dbt/seeds/dim_wiki.csv](dbt/seeds/dim_wiki.csv): the header is set; fill it from the sitematrix API (a small script in `ingest/` is fine) |
| S9 dashboard (later) | Metabase at http://localhost:3000 already has the "Wiki serving" connection and a first chart on dashboard "Wikipedia edits" (created by `python scripts/metabase_setup.py`). Build new tiles there on the `mart_*` tables |
| S6 diagram | [docs/design.md](docs/design.md) §3–§6; save files in `docs/diagram/` |

## 5. Git workflow

- `main` always works. Work on a branch (`sumaiya/pageviews`, `lynn/minio`), open a PR, and merge when it runs.
- **Never commit data or `.env`.** Both are git-ignored. Data lives in Docker volumes.
- Small commits with clear messages (`ingest: filter pageviews to Wikipedia projects`).
- **Mac + Windows rules:** use `pathlib` for file paths in Python (never hard-code `\` or `/data/...` on
  your laptop); any new Docker image must have an `arm64` build (check with
  `docker manifest inspect <image> | grep arm64`); line endings are fixed to LF by `.gitattributes`.

## 6. Daily update (group chat, by 10 pm)

```
✅ Done today:   (task IDs)
🚧 Blocked on:
➡️ Tomorrow:     (task IDs)
```
Lynn adds the collector line from `python -m stream.status` until the analysis window closes (11 Nov).
