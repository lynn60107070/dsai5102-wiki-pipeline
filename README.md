# Wikipedia live edits pipeline

DSAI5102 Data Architecture & Engineering, final project · Lynn Younes & Sumaiya

**Question:** Wikipedia's volunteer patrollers have limited time: where does editing pressure
concentrate, how much of it comes from bots, and do spikes in reader attention cause edit surges?

An end-to-end pipeline that streams every Wikipedia edit live through Kafka, loads hourly pageview
files in batch with Airflow, models both as a star schema with dbt on DuckDB, and serves a Metabase dashboard.

> **Status: work in progress.** The stream collector is live. The rest is being built (see [SCHEDULE.md](SCHEDULE.md)).

## Architecture (target)

| Layer | Tool | Job |
|---|---|---|
| Containers | Docker Compose | Starts everything with one command |
| Streaming | Apache Kafka | Live edit events: `wiki.raw` → processor → bot / human / per-minute topics |
| Data lake | MinIO | Raw and cleaned files, partitioned by date and hour |
| Orchestration | Apache Airflow | Hourly batch: pageview files → lake → dbt → dashboard tables |
| Warehouse + transforms | DuckDB + dbt | Star schema (`fact_edit`, `fact_pageviews_hourly`, dimensions) and marts |
| Serving | PostgreSQL | Holds the final dashboard tables for Metabase |
| Dashboard | Metabase | Edits over time, bot share, top languages, attention vs edits |

Details: [docs/design.md](docs/design.md).

## Quick start

Requirements: Docker Desktop (8 GB+ memory) and Git, on macOS (Intel or Apple Silicon), Windows or Linux.

```bash
cp .env.example .env          # Windows: copy .env.example .env
docker compose up -d --build
docker compose exec raw-sink python -m stream.status
```

| Service | What it does |
|---|---|
| `broker` | Kafka (KRaft, single broker) |
| `kafka-init` | Creates the topics, then exits |
| `producer` | Reads Wikimedia EventStreams, keeps edits + new pages, writes to `wiki.raw` keyed by wiki |
| `raw-sink` | Consumes `wiki.raw`, writes gzipped JSON-lines files to the lake: `s3://lake/raw/edits/dt=/hour=/` |
| `minio` | Data lake (S3 API on `:9000`). Web console: http://localhost:9001, log in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |

Stop with `docker compose down` (data is kept). `docker compose down -v` also deletes the data.

## Tests

```bash
pip install -r requirements.txt
python -m unittest -v
```

## Data sources

- Wikimedia EventStreams `recentchange`: https://stream.wikimedia.org
- Wikipedia pageview dumps: https://dumps.wikimedia.org/other/pageviews/
- Wikimedia sitematrix API

(Licences for each source are confirmed and cited in the report.)
