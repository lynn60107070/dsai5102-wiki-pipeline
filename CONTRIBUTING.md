# Team guide: start here

The whole pipeline is built and running: live Wikipedia edits through Kafka into the MinIO lake, hourly
pageviews through Airflow, a dbt star schema on DuckDB rebuilt every hour, and a Metabase dashboard.
What's left is mostly **diagram, dashboard, insights, report and slides**.
How it works and why: **[README.md](README.md)** (start here) and **[docs/design.md](docs/design.md)** (details).

## 1. One-time setup (macOS or Windows)

Everything runs in Docker, and every image has both Intel/AMD and Apple Silicon builds.

1. **Accept the GitHub invite** (email, or https://github.com/lynn60107070/dsai5102-wiki-pipeline/invitations):
   the repo is private, so cloning fails until you do.
2. **Docker Desktop** → Settings → Resources: give it **at least 8 GB of memory**.
3. Clone and start everything (no configuration needed; a `.env` file is optional):
   ```bash
   git clone https://github.com/lynn60107070/dsai5102-wiki-pipeline.git
   cd dsai5102-wiki-pipeline
   docker compose up -d --build
   docker compose logs -f bootstrap        # ends with "ready: ..." after ~3 min; Ctrl+C to stop watching
   ```
4. Open the **dashboard**: http://localhost:3000 → *Wikipedia edits*. Logins for the dashboard,
   Airflow and MinIO are in the README's Quick start table.

Your copy starts with **6 hours of real sample data** (bundled in the repo) plus whatever your own
stream collects while it runs. **Only Lynn's laptop collects the project's data 24/7**, so on yours run
it while you work, then `docker compose down` (add `-v` to also delete your local data).

Optional, only for running the Python tests or scripts on your laptop (Python 3.12; on a Mac without it:
`brew install python@3.12`):
```bash
python3.12 -m venv .venv && source .venv/bin/activate      # Windows: py -3.12 -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
python -m unittest -v
```

## 2. Who owns what

You edit only the files you own. To change someone else's file, ask them or open a PR they review.

| Owner | Files | Job |
|---|---|---|
| **Sumaiya** | `docs/diagram/`, `metabase/`, `paper/sections/01`, `02`, `06`, `07` | Diagram, tool table, dashboard, insights, half the report and slides |
| **Lynn** (lead) | everything else: `docker-compose.yml`, `stream/`, `ingest/`, `airflow/`, `dbt/`, `scripts/`, `postgres/`, `sample/`, `tests/`, `README.md`, `docs/design.md`, `paper/main.tex` + `paper/sections/03`–`05` (+ the Overleaf project) | Architecture and all pipeline code, report assembly |
| **Both** | `CONTRIBUTING.md`, `paper/refs.bib` (add only) | Change only when both agree |

**The paper** is written the same way as the code (branches + PRs, one file per section); see
[paper/README.md](paper/README.md).

## 3. Where your tasks start

| Task | Start here |
|---|---|
| S1 setup | Section 1 above. Post a screenshot of the dashboard + `docker compose ps` in the chat |
| S5 ask Dr. Bouguelia | Presentation date (25 Nov or 2 Dec?), report template preference, 2-person group OK |
| S6 diagram | The README's text diagram + [docs/design.md](docs/design.md) §3–§5. Draw it in draw.io; save `.drawio` + `.svg` + `.png` + `.pdf` in `docs/diagram/` |
| S7 tool table | README's "Layer / Tool / Job / Why" table is a start; add 1–2 alternatives per tool and why we didn't pick them. Goes in [paper/sections/04_design_choices.tex](paper/sections/04_design_choices.tex) at the `\todo` |
| S9 dashboard | Metabase at http://localhost:3000 already has the "Wiki serving" connection and two tiles. Build new tiles on the `mart_*` tables (each table's grain and meaning: [docs/design.md](docs/design.md) §6). When a tile is final, add its SQL to `CARDS` in [scripts/metabase_setup.py](scripts/metabase_setup.py) (or send it to Lynn) so a fresh install recreates it |
| S8, S10, S11 | After the analysis window closes (11 Nov): numbers from the dashboard/marts into the report sections |

Useful while exploring the data: the Postgres database `serving` (host port **5433**, user `metabase_ro`,
password `METABASE_DB_PASSWORD` from `.env.example`) holds exactly the tables the dashboard sees.

## 4. Git workflow

- `main` always works. Work on a branch (`sumaiya/diagram`, `sumaiya/paper-intro`), open a PR into `main`.
- **Never commit data or `.env`.** Both are git-ignored. Data lives in Docker volumes.
- Small commits with clear messages (`diagram: v1 architecture`).
- **Mac + Windows rules:** use `pathlib` for file paths in Python; any new Docker image must have an
  `arm64` build (`docker manifest inspect <image> | grep arm64`); line endings are fixed to LF by `.gitattributes`.

## 5. Daily update (group chat, by 10 pm)

```
✅ Done today:   (task IDs)
🚧 Blocked on:
➡️ Tomorrow:     (task IDs)
```
Lynn adds the collector line from `python -m stream.status` until the analysis window closes (11 Nov).
