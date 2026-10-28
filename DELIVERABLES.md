# Deliverables

Everything we must hand in, mapped to the rubric row it earns marks on, with an owner and a due checkpoint.
Tick items off as they are **finished**. Before CP6 and before submitting, mark ourselves against every rubric row:
anything not honestly in the "Good" column is the cheapest mark available.

Source: `requirements_rubrics/Project Description - End-to-End Data Pipeline.pdf` and the course guide
(project = 40% of the course; submission due **Tue 24 Nov 2026**; presentations in weeks 14–15).
Checkpoint dates are in [SCHEDULE.md](SCHEDULE.md).

## 0. Decided / built
- [x] Pipeline built and running end to end (25–26 Oct): see SCHEDULE.md "Where we are"
- [x] Topic: Wikipedia live edits (stream) + hourly pageview files (batch) + sitematrix (reference data)
- [x] Stack: Docker Compose · Kafka · MinIO · Airflow · DuckDB + dbt · Postgres (dashboard tables only) · Metabase
- [ ] Problem statement agreed as one sentence (L3, CP1). Working version: *Wikipedia's volunteer patrollers
      have limited time: where does editing pressure concentrate, how much of it comes from bots, and do
      spikes in reader attention cause edit surges?*

## 1. Presentation: ≈15 min + 5 min Q&A · **Wed 25 Nov** (confirmed 28 Oct; live demo optional, our choice)

| Deliverable | Rubric row | Owner | Due |
|---|---|---|---|
| Slides: the problem and who it matters to, in the first minute; dataset (sources, volume, stream vs batch) | Problem Description | Sumaiya | CP5 |
| Slides: architecture diagram + what each tool does and why (alternatives considered) | Presentation, Documentation | Lynn | CP5 |
| Slides: stream (topics, processor, consumer group), batch DAG, star schema, dbt lineage | Ingestion, Warehouse, Transformations | Lynn | CP5 |
| Pipeline demo (live, optional per the brief) + backup video | Presentation | Lynn | Rehearsal 2 |
| Slides: dashboard + 3–5 insights with numbers | Dashboard | Sumaiya | CP5 |
| Slides: challenges, design decisions, lessons learned (collector gaps, DuckDB locking → Postgres for serving, volume filtering) | Presentation | Both | CP5 |
| Q&A prep sheet (S14); both can explain the whole system | Presentation & Communication | Both | CP6 |

Speaking order (draft): Sumaiya (problem + data, ~2 min) → Lynn (architecture + pipeline + demo, ~7 min) →
Sumaiya (dashboard + insights, ~4 min) → Lynn (challenges + lessons + conclusion, ~2 min).

## 2. GitHub repository · **Tue 24 Nov**

| Deliverable | Rubric row | Owner | Due |
|---|---|---|---|
| Pipeline code: `stream/` (producer, processor, sinks), `ingest/` (pageviews, wiki names), `dbt/` (models, seeds, tests) | Ingestion, Transformations | Lynn | ✅ 26 Oct |
| Orchestration: `airflow/dags/` (`pageviews_hourly`, `warehouse_hourly`) | Ingestion (batch) | Lynn | ✅ 25 Oct |
| `docker-compose.yml` that starts everything with one command; pinned image tags; `.env` optional | Reproducibility | Lynn | ✅ 25 Oct |
| Sample-data mode (6 bundled hours) so the dashboard works without waiting for the stream; regenerate from a window day | Reproducibility | Lynn | ✅ / CP4 |
| Dashboard recreated on a fresh install: final tiles' SQL in `CARDS` in `scripts/metabase_setup.py` | Dashboard, Reproducibility | Sumaiya (SQL) + Lynn (merge) | CP4 |
| `README.md`: architecture + diagram, what each container does, setup, run (sample + live), troubleshooting | Reproducibility, Documentation | Lynn (diagram image from S6) | ✅ v1 / CP4 |
| Fresh-clone test passed on a second laptop (S13) | Reproducibility | Sumaiya | CP6 |
| Repo **made public** at submission (instructor's OK: public or invited, our choice). **Check before submitting** | — | Lynn | Submit |

## 3. Report (PDF, 5–10 pages **excluding references and appendices**, any template, LaTeX in `paper/`, compiled on Overleaf) · **Tue 24 Nov**, one submission per group

Written through GitHub PRs, one file per section; workflow in [paper/README.md](paper/README.md).

| Section (required by the brief) | Rubric row | Owner | Due |
|---|---|---|---|
| Title, both names + student IDs, course, repo link | Documentation | Lynn | CP5 |
| Problem statement + motivation (who is affected, what decision it supports) | Problem Description | Sumaiya | CP5 |
| Dataset: the three sources, how often each updates, volume, data profile table (S8) | Problem Description | Sumaiya | CP5 |
| Architecture overview **with diagram** (S6) | Documentation | Lynn | CP5 |
| Design choices + justification: tool table with alternatives (S7), why stream + batch, why DuckDB computes and Postgres serves | Documentation, Warehouse | Lynn | CP5 |
| Implementation: data flow, Kafka topics + consumer group, lake layout, star schema (tables + grain), dbt models + tests, Airflow DAG | Ingestion, Warehouse, Transformations | Lynn | CP5 |
| Results + insights: dashboard screenshots, 3–5 findings with numbers | Dashboard | Sumaiya | CP5 |
| Discussion + conclusion: limitations (laptop collector gaps, 2-week window, single broker, bot flag is self-reported), possible extensions (topics via Wikidata, cloud deployment, alerting) | Documentation | Sumaiya | CP5 |
| References: Wikimedia data sources + licence (CC BY-SA / CC0), tool docs | Documentation | Lynn | Submit |
| AI-use acknowledgement (if required) | Academic integrity | Lynn | Submit |
| Every figure/table numbered, captioned and referenced in the text | Documentation | Both | CP6 |

## 4. Rubric self-check: aim for "Good" in every row (T3, CP6)

| Rubric row | What "Good" requires | How we hit it |
|---|---|---|
| Problem Description | Clear motivation and what it solves | Patroller-workload framing, one-sentence question, a named audience |
| Ingestion (batch) | End-to-end DAG, multiple steps, uploads to storage | Airflow: download → filter → MinIO → dbt build → export, hourly with retries |
| Ingestion (stream) | Producers/consumers + streaming technologies, not just 1 → 1 | Producer → processor → 3 topics → sink consumer group (2 replicas) + dead-letter topic |
| Warehouse | Well-designed schema optimised for queries, justified | Star schema (2 facts, 4 dims), partitioned Parquet, marts per dashboard tile, explained in the report |
| Transformations | dbt/Spark, clean analytics-ready data | dbt staging → facts/dims → marts, incremental models, tests |
| Dashboard | 2+ meaningful tiles with labels, titles, explanations | 5 tiles, each with a title + one-line explanation, date/wiki filters |
| Reproducibility | Clear instructions, easy to run, it works | One `docker compose up`, sample-data mode, fresh-clone tested |
| Documentation | Clear report + README with diagrams and examples | Diagram, tool table, schema table, dbt lineage graph, screenshots |
| Presentation | Clear, well-prepared, answers questions confidently | 2 rehearsals, demo + backup video, Q&A sheet |

## 5. Final pre-submission checklist
- [ ] `docker compose up` on a fresh clone brings up every service (sample mode + live mode)
- [ ] All dbt tests pass; the Airflow DAG shows green runs
- [ ] Dashboard has ≥ 2 tiles (we have 5), each with a title and explanation
- [ ] README has setup instructions + architecture explanation + diagram
- [ ] Report PDF is 5–10 pages (references and appendices not counted) and covers all six required parts (problem, architecture, design choices, implementation, results, discussion)
- [ ] Repo link in the report, and the instructor can open it
- [ ] Slides final; backup demo video saved locally
- [ ] Submitted by Tue 24 Nov
