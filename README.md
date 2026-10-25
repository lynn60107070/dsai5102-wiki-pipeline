# Wikipedia live edits pipeline

DSAI5102 Data Architecture & Engineering, final project · Lynn Younes & Sumaiya

**Question:** Wikipedia's volunteer patrollers have limited time: where does editing pressure
concentrate, how much of it comes from bots, and do spikes in reader attention cause edit surges?

An end-to-end data pipeline that streams every edit on every Wikimedia wiki live through Kafka, loads
hourly pageview files in batch with Airflow, keeps both in a MinIO data lake, models them as a star
schema with dbt on DuckDB, and serves a Metabase dashboard, including a near-real-time tile.

## Quick start (one command)

Requirements: **Docker Desktop with at least 8 GB of memory** (Settings → Resources) and Git, on macOS
(Intel or Apple Silicon), Windows or Linux.

```bash
git clone https://github.com/lynn60107070/dsai5102-wiki-pipeline.git
cd dsai5102-wiki-pipeline
docker compose up -d --build
docker compose logs -f bootstrap        # optional: watch the setup; ends with "ready: ..." (~5 min)
```

No configuration is needed: every setting has a working default (`.env.example`). To change passwords,
copy `.env.example` to `.env` and edit it before the first `up`.

**What you'll see:** a fresh install has no history yet, so the one-off `bootstrap` service loads
**6 bundled hours of real data** (`sample/`, about 390k edits, no user names) into the empty lake, builds
the warehouse once and sets up the dashboard. At the same time the live stream starts collecting, and
the live tile fills within minutes. The sample is only ever loaded into an empty lake.

| Open | Address | Login (defaults) |
|---|---|---|
| **Dashboard** (Metabase) | http://localhost:3000 → *Wikipedia edits* | `admin@example.com` / `change-me-Metabase-123` |
| Orchestration (Airflow) | http://localhost:8080 | `admin` / `change-me-airflow-123` |
| Data lake (MinIO) | http://localhost:9001 | `lakeadmin` / `change-me-lake-123` |

Add `#refresh=60` to the dashboard URL to auto-refresh the live tile.

## Architecture

```
                       ┌──────────────────────────── stream (always on, Docker Compose services) ─────────────────────────┐
Wikimedia EventStreams ─► producer ─► [wiki.raw] ─┬─► raw-sink ──────────────────────────────► lake: raw/edits  (as received)
  (~16 edits/s kept)                              └─► processor ─┬─► [edits.bot]  ┐
                                                                 ├─► [edits.human] ┴─► clean-sink ×2 ─► lake: clean/edits (Parquet)
                                                                 ├─► [edits.per_minute] ─► live-stats ─► Postgres serving ─┐
                                                                 └─► [wiki.deadletter]                                     │
                       ┌──────────────────────────── batch (Airflow) ───────────────────────────────────────────────────┐  │
pageview dumps (hourly) ─► pageviews_hourly: wait → fetch → upload ─► lake: raw/pageviews ── Dataset ─┐                  │  │
                          warehouse_hourly (hourly + on new pageviews):                               ▼                  │  │
                            compact clean zone → dbt build (DuckDB star schema + 38 tests) → export ─► Postgres serving ─┼──┤
                                                                                                                          ▼  ▼
                                                                                              Metabase (read-only user)
```

| Layer | Tool | Job | Why this tool |
|---|---|---|---|
| Containers | Docker Compose | Starts all 14 services with one command | Reproducible on any laptop; taught in lab 5 |
| Streaming | Apache Kafka 8 (KRaft) | 5 topics, 4 consumer groups | Durable, replayable event log; taught in lab 10 |
| Stream processing | Python services | Validate, remove user names, split bot/human, per-minute counts | Simple, unit-tested; enough for ~16 events/s |
| Data lake | MinIO (S3 API) | Raw + clean zones, partitioned `dt=/hour=` | S3-compatible locally; as in the course demo |
| Orchestration | Apache Airflow 2.11 | Pageview ingestion, warehouse refresh | Taught in lab 8; Datasets for data-aware scheduling |
| Warehouse | DuckDB | Star schema, columnar analytics | Fast on a laptop, reads Parquet on S3 directly |
| Transformations | dbt | Staging → core (facts, dims) → marts, tests | Versioned, tested SQL; incremental models |
| Serving | PostgreSQL 16 | Dashboard tables, read-only user | DuckDB allows one writer, so the dashboard reads copies |
| Dashboard | Metabase | Tiles on the marts + a live tile | Free, quick, scripted setup |

Design decisions, measurements and trade-offs: **[docs/design.md](docs/design.md)**.

## Services

| Service | What it does |
|---|---|
| `broker`, `kafka-init` | Kafka (single broker, KRaft); `kafka-init` creates the topics, then exits |
| `producer` | Reads Wikimedia EventStreams, keeps edits + new pages, writes to `wiki.raw` keyed by wiki; resumes from its last event after a restart |
| `raw-sink` | `wiki.raw` → `s3://lake/raw/edits/dt=/hour=/` (gzipped JSON lines, exactly as received) |
| `processor` | `wiki.raw` → validates, removes user names, splits into `edits.bot` / `edits.human`, counts edits per minute (`edits.per_minute`); failures → `wiki.deadletter` |
| `clean-sink` (×2) | Consumer group on `edits.bot` + `edits.human` → Parquet in `s3://lake/clean/edits/dt=/hour=/`. Scale: `docker compose up -d --scale clean-sink=3` |
| `live-stats` | `edits.per_minute` → Postgres `serving.live_edits_per_minute` (the live tile) |
| `minio` | Data lake (S3 API on `:9000`, console on `:9001`) |
| `postgres` | Databases `airflow` (metadata), `serving` (dashboard tables), `metabase` (Metabase settings). Host port `5433` |
| `airflow-init`, `airflow-webserver`, `airflow-scheduler` | Airflow (LocalExecutor). The image also contains dbt + DuckDB |
| `metabase` | Dashboard; reads `serving` as the read-only user `metabase_ro` |
| `bootstrap` | One-off setup at the end of every `up`: sample data (empty lake only), first warehouse run, Metabase setup |

**DAGs:** `warehouse_hourly` (on by default) and `pageviews_hourly` (paused until the pageview ingestion
is finished; then unpause it in the Airflow UI and set `pageviews_enabled: true` in `dbt/dbt_project.yml`).

## Repository layout

```
docker-compose.yml   all services          stream/     producer, processor, sinks, status (Python)
airflow/             DAGs + image (dbt)     ingest/     pageview download (batch source)
dbt/                 models, seeds, tests   scripts/    bootstrap, export, compaction, Metabase setup, checks
postgres/init/       databases + roles      sample/     6 hours of real data for a fresh install
tests/               unit tests             docs/       design note + diagram      paper/   report (LaTeX)
```

## Operating it

| Task | Command |
|---|---|
| Collector status (events per hour, gaps) | `docker compose exec raw-sink python -m stream.status` |
| Follow a service | `docker compose logs -f processor` |
| Rebuild the warehouse now | trigger `warehouse_hourly` in the Airflow UI |
| Stop (data is kept) | `docker compose down` |
| Stop and delete all data | `docker compose down -v` |

Running a warehouse step by hand (not while `warehouse_hourly` is running: DuckDB allows one writer):
```bash
docker compose exec airflow-scheduler bash -c 'cd $DBT_PROJECT_DIR && /opt/airflow/dbt-venv/bin/dbt build'
docker compose exec airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/export_marts.py
```

## Troubleshooting

| Problem | Fix |
|---|---|
| A port is already in use (5433, 8080, 3000, 9000, 9001, 9092) | Stop whatever uses it, or change the left side of that service's `ports:` in `docker-compose.yml` |
| Containers restart or Docker is very slow | Give Docker Desktop at least 8 GB of memory; the stack uses about 4.5 GB |
| `bootstrap` exited with an error | `docker compose logs bootstrap`, then `docker compose up -d` to run it again (it is safe to re-run) |
| Git Bash on Windows rewrites `/opt/...` paths | Run `export MSYS_NO_PATHCONV=1` first |
| Live tile is empty | Normal for the first ~3 minutes after the stream starts |

## Tests

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m unittest -v                                  # 14 unit tests, no Docker needed
```
Data tests (38) run inside every `dbt build`.

## Data sources

- Wikimedia EventStreams `recentchange`: https://stream.wikimedia.org
- Wikipedia pageview dumps: https://dumps.wikimedia.org/other/pageviews/
- Wikimedia sitematrix API (wiki and language names)

Licences for each source are confirmed and cited in the report. Team workflow: [CONTRIBUTING.md](CONTRIBUTING.md).
