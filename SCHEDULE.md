# Schedule, tasks and checkpoints

**Project:** Wikipedia live edits pipeline (bots vs humans, edit pressure vs attention).
**Team:** Lynn Younes (lead) · Sumaiya

**Submission is due Tue 24 Nov** (GitHub repo + report PDF). Presentations are in the final sessions
(week 14 or 15, so Wed 25 Nov or Wed 2 Dec; S5 asks Dr. Bouguelia which).

**Goal: 95% of the project done by Thu 19 Nov.** By then the pipeline runs end to end with one command,
the dashboard is final, and the full report and slides have been reviewed by both of us. After 19 Nov
we only polish, rehearse and submit.

**The stream must start this week.** The live edit history only builds up from the day the collector
starts, so L2 (collector live by **Tue 27 Oct**) is the most time-critical task in the plan. Everything
we analyse comes from the **analysis window: Wed 28 Oct 00:00 UTC to Wed 11 Nov 23:59 UTC** (two full weeks,
so weekday/weekend patterns show). The window is frozen at CP4 so the report numbers stop moving.
The stream keeps running afterwards for the live demo. All timestamps are **UTC** (the pageview files are in UTC).

Each person owns separate folders, so **nobody waits for anyone**. Where one task feeds another, the
receiving task has a fallback (see the dependency notes under each table).

Effort estimates are hands-on time. Each task's tag says why it exists: **R:** = the rubric row it earns
marks on, **Lec n** = the lecture or lab it applies. The full deliverables list is in [DELIVERABLES.md](DELIVERABLES.md).

---

## Timeline

| Checkpoint | Date | Where the team should be |
|---|---|---|
| Kick-off | Sun 25 Oct | Plan shared; both read this file and DELIVERABLES.md |
| **CP0** | **Mon 26 Oct** | **Both set up**: repo cloned, Docker Desktop running, the Kafka lab compose starts (proof in the chat, no call) |
| Collector | **Tue 27 Oct** | **Stream collector live** and writing data 24/7 (L2) |
| **CP1** | **Thu 29 Oct** | Problem statement, topics, lake layout and star schema agreed (L3); pageview download script works on one hour (S2); `dim_wiki` seed (S3) |
| **CP2** | **Fri 6 Nov** | **Thin slice works end to end**: Kafka → MinIO → dbt/DuckDB → Postgres → one Metabase chart; Airflow pageview DAG v0 runs; diagram v1 |
| **CP3** | **Tue 10 Nov** | **Pipeline frozen**: stream processor + split topics + consumer group, full star schema + dbt tests, full Airflow DAG. Only bug fixes after this |
| **CP4** | **Fri 13 Nov** | **Analysis window frozen**; dashboard final; insights with numbers; one-command setup + sample-data mode; README v1 |
| **CP5** | **Mon 16 Nov** | **Full report draft** (every section) + **slide draft** |
| **CP6** | **Thu 19 Nov** | **95% done**: report + slides reviewed by both, rubric self-check, fresh-clone test passed, **rehearsal 1** (timed 15 min + mock Q&A) |
| Polish | 20–23 Nov | Fix review comments only (Lynn's DSML presentation is Sun 22 Nov) |
| Rehearsal 2 | Mon 23 Nov | Second timed run; **slides locked**; backup demo video recorded |
| **Submit** | **Tue 24 Nov** | Repo link + report PDF submitted |
| **Presentation** | Wed 25 Nov or Wed 2 Dec | ~15 min + 5 min Q&A, both speak |

**Integration time is built in.** CP2 → CP3 turns the thin slice into the full pipeline, CP3 → CP4 is
dashboard + analysis, CP4 → CP5 is writing, CP5 → CP6 is review. 20–23 Nov is buffer.

### Why the checkpoints sit where they do (Lynn's other deadlines)
| Date | Other deadline |
|---|---|
| Wed 28 Oct · Fri 30 Oct · Sun 1 Nov | AICC CP4 · CP5 · CP6 (submission-ready) |
| Wed 28 Oct | ARCH Kafka lab |
| Thu 29 Oct | DSML CP2 |
| Tue 3 Nov | AICC course progress check |
| Wed 4 Nov | DSML CP3 · ARCH Spark lab |
| Sun 8 Nov · Wed 11 Nov · Sat 14 Nov | DSML CP4 · CP5 · CP6 |
| Tue 17 Nov | AICC deadline (light, feedback only) |
| Wed 18 · Fri 20 · Sun 22 Nov | DSML rehearsal 1 · rehearsal 2 · **presentation** |

So until 1 Nov, Lynn only does the time-critical collector (L2) and the design (L3), and Sumaiya
builds the batch-side pieces in parallel. Lynn's heavy build is 2–10 Nov.

**Daily update** in the group chat, every day by 10 pm (three lines, even if nothing changed):
```
✅ Done today:   (task IDs)
🚧 Blocked on:
➡️ Tomorrow:     (task IDs)
```
Also post **one line on the collector** (running / gap from X to Y) until the window closes on 11 Nov.

**Collector always-on period: until Wed 11 Nov 23:59 UTC** (Thu 12 Nov, 02:59 Qatar time). Until then
Lynn's laptop stays plugged in, locked (never signed out) and online; sleep, hibernate and lid-close are off
when plugged in, Docker starts on sign-in, and Windows Update is paused until 25 Nov. Short unplugged trips
are fine (the producer catches up after waking). After 11 Nov it only runs while building and on demo day
(start ~30 min before the presentation).
Blocked? Message **immediately**. Don't wait for the evening.

**Checkpoint call (15 min):** each person shows the tasks due (a container that runs, a DAG run in the
Airflow UI, a chart, a draft section), and we agree on the focus until the next checkpoint.
Done means finished, not started.

---

## Tasks

### Lynn (lead): architecture, streaming, lake, orchestration, warehouse + dbt, integration
Owns `docker-compose.yml`, `stream/`, `airflow/dags/`, `dbt/models/`, `scripts/`, `README.md`, `paper/main.tex`, `paper/sections/03`–`05` (+ the Overleaf project).

| ID | Task | Effort | Due |
|---|---|---|---|
| L1 | ✅ Done 25 Oct (private repo `lynn60107070/dsai5102-wiki-pipeline`, Sumaiya invited). GitHub repo + folder layout, add Sumaiya, `.gitignore` for data and `.env`. **Data lives in Docker named volumes, never in the OneDrive folder** (OneDrive sync + millions of small files = locks and slowdowns) | 45 min | **CP0** |
| L2 | ✅ **Live since 25 Oct 05:58 UTC** (~950 events/min kept, resume after restart tested). **Collector live**: compose with Kafka (single broker, from the lab) + producer reading the EventStreams `recentchange` feed into topic `wiki.raw` (key = wiki, so each wiki stays in one partition) + a raw sink consumer writing hourly `.jsonl.gz` files. Keep only `edit` + `new` events and drop `canary` events (~14/s, ~1.2M/day). Laptop set to **never sleep when plugged in**; test whether `?since=` can backfill short gaps [R: Stream, Lec 10] | 3 h | **Tue 27 Oct** |
| L3 | Design note (`docs/design.md`): problem statement as one sentence, topic list, lake layout (`raw/` → `clean/`, partitioned `dt=/hour=`), star schema draft (table, grain, keys). Shared with Sumaiya for the diagram (S6) [R: Problem, Warehouse, Lec 4 star schema] | 2 h | **CP1** |
| L4 | ✅ **Done 25 Oct** (pinned `bitnamilegacy/minio`, since free official images are gone; raw zone kept as `.jsonl.gz`, Parquet moves to the clean zone in L9; 62,890 pre-MinIO events backfilled, DuckDB reads the lake). MinIO in compose (**pin an image tag**, no `latest`); sink writes Parquet to `s3://lake/raw/edits/dt=/hour=`; backfill the `.jsonl.gz` files collected since 27 Oct [R: Warehouse, Lec 5 data lake, Lec 8 ELT] | 3 h | **CP2** |
| L5 | ✅ **Done 25 Oct** (Airflow 2.11.2, LocalExecutor, Postgres on host port 5433; DAG tested up to the S2 stub, upload tested against MinIO; DAG stays paused until S2 is merged). Airflow in compose (LocalExecutor, Postgres metadata DB) + `pageviews_hourly` DAG v0 using S2's function: download → filter → MinIO [R: Batch orchestration, Lec 8 + Airflow lab] | 3 h | **CP2** |
| L6 | ✅ **Done 25 Oct**: thin slice works end to end (`stg_edits` + `mart_edits_hourly`, 7 dbt tests pass; export to Postgres `serving`; read-only Metabase user; chart "Edits per hour: bots vs humans" on dashboard "Wikipedia edits"). `stg_pageviews` waits for S2. dbt-duckdb project: sources on the MinIO Parquet, `stg_edits`, `stg_pageviews`, one mart, export to Postgres, connect Metabase, one chart → **thin slice** | 3 h | **CP2** |
| L7 | ✅ **Done 25 Oct**: processor (validation, privacy, bot/human split, per-minute counts with a per-partition watermark), clean-sink consumer group ×2 writing Parquet, live-stats → Postgres + live Metabase tile, dead-letter topic; clean-zone compaction script (removed 381k restart duplicates, 0 events missing); 60 s graceful shutdown. 14 unit tests. Stream processor: consume `wiki.raw` → validate + clean → `edits.bot` / `edits.human` + per-minute counts in `edits.per_minute`; bad events to `wiki.deadletter`; run the lake sink as a **consumer group with 2 replicas** [R: Stream: "Good" needs more than one producer → one consumer] | 4 h | **CP3** |
| L8 | ✅ **Done 25 Oct**: full star schema (2 + 2 facts, 4 dims, 4 marts), dbt reads the clean Parquet zone, incremental facts, 38 tests (49 with pageviews), pageview models tested on a fixture and waiting for S2 (`pageviews_enabled`). Star schema in dbt: `fact_edit` (one row per edit), `fact_pageviews_hourly`, `dim_wiki` (S3 seed), `dim_article`, `dim_time`, `dim_editor_type`; marts `mart_edits_hourly`, `mart_bot_share`, `mart_attention_vs_edits` (edits vs pageviews by article-hour, spike flag), `mart_data_completeness` (hours with gaps); incremental models; dbt tests (`unique`, `not_null`, `relationships`, `accepted_values`) [R: Warehouse, Transformations, Lec 4] | 6 h | **CP3** |
| L9 | ✅ **Done 25 Oct**: DAG `warehouse_hourly` (freshness check, compaction, dbt build as quality gate, export), hourly + Dataset-triggered by pageviews, runs in ~20 s, unpaused. Full Airflow DAG: pageviews ingest → `dbt build` (run + test) → export marts to Postgres; hourly, with retries; daily task that merges the small Parquet files [R: Batch orchestration: "multiple steps in the DAG"] | 3 h | **CP3** |
| L10 | ✅ **Done 25 Oct**: `.env` optional, `bootstrap` service (sample into an empty lake only, first warehouse run via Airflow, Metabase setup), 6-hour sample; fresh-clone test found and fixed 2 bugs (Windows path length, live tile before the stream). Regenerate the sample from a window day after 11 Nov (`scripts/make_sample.py`). One-command setup: `docker compose up` brings everything up; `.env.example`; **sample-data mode** (one bundled day of Parquet) so the instructor can see the dashboard without waiting days for the stream [R: Reproducibility] | 3 h | **CP4** |
| L11 | ✅ **v1 done 25 Oct** (quick start, logins, architecture, services, operations, troubleshooting); add S6's diagram image at CP4. README v1: architecture diagram (S6), what each container does, setup, run, sample mode, troubleshooting [R: Reproducibility, Documentation] | 2 h | **CP4** |
| L12 | 🚧 **Drafted 25 Oct** (`paper/sections/03`–`05`); still to add: S6 diagram, S7 tool table, dbt lineage figure, final numbers from the window. Report: architecture overview, design choices + justification (uses S7's table), implementation details (data flow, topics, schema, DAG, dbt lineage graph) [R: Documentation] | 5 h | **CP5** |
| L13 | Slides: template, architecture, pipeline walkthrough, design decisions (~7 min); assemble the deck | 2 h | **CP5** |
| L14 | 🚧 **Script written 25 Oct** ([docs/demo.md](docs/demo.md)); record the backup video at Rehearsal 2. Live demo script (what to click, in order) + backup screen recording in case the demo fails | 1 h | Rehearsal 2 |
| L15 | Final package: repo access for the instructor checked, report PDF, submit | 1 h | **Tue 24 Nov** |
| L16 | Paper setup **before handover**: LaTeX skeleton in `paper/` (one file per section, workflow in `paper/README.md`) + Lynn's Overleaf project that compiles the PDF. Sections arrive by PR; Lynn compile-checks, merges, copies into Overleaf | 1 h | **Sun 25 Oct** |

### Sumaiya: batch ingestion pieces, reference data, diagram, dashboard, insights
Owns `ingest/`, `dbt/seeds/`, `docs/diagram/`, `metabase/`.

| ID | Task | Effort | Due |
|---|---|---|---|
| S1 | Setup: clone, Docker Desktop with **≥ 8 GB RAM** allotted, start the Kafka lab compose, post a screenshot | 30 min | **CP0** |
| S2 | `ingest/pageviews.py`: `fetch_hour(date, hour)` downloads one hourly file from `dumps.wikimedia.org/other/pageviews/`, keeps only Wikipedia projects, writes Parquet. Test on one hour; record file size and rows before → after the filter (goes in the report) [R: Batch, Lec 3 data sources] | 3 h | **CP1** |
| S3 | `dbt/seeds/dim_wiki_names.csv` (renamed 25 Oct: the `dim_wiki` model now joins it to the wikis seen in the stream) from the sitematrix API (`action=sitematrix`): wiki id (`enwiki`), language code, language name, project family, Arabic/local flag [R: Warehouse] | 1.5 h | **CP1** |
| S5 | Ask Dr. Bouguelia: our presentation date, any report template preference, a 2-person group is fine | 10 min | **CP1** |
| S6 | Architecture diagram (draw.io, export PNG + SVG) from L3: v1 at CP2, final at CP4 [R: Documentation: "with diagram"] | 2 h | **CP2** / **CP4** |
| S7 | Tool justification table: for each tool, its job, 1–2 alternatives, why we picked it (stress the 2 not covered in class: DuckDB, dbt) [R: "explain tools not covered in the course"] | 2 h | **CP2** |
| S8 | Data profile from the marts: events per day, bot share, duplicates removed, gap hours (`mart_data_completeness`), storage size raw vs Parquet → one table for the report | 2 h | **CP4** |
| S9 | Metabase dashboard on Lynn's marts, **5 tiles**, each with a title + one-line explanation: (1) edits per minute over time, bot vs human; (2) bot share by wiki (top 15); (3) top languages by edits; (4) most-edited articles vs their pageviews (attention vs edits); (5) data completeness by hour. Filters: date range, wiki [R: Dashboard] | 4 h | **CP4** |
| S10 | Insights: 3–5 findings with numbers (e.g. "X% of edits are bots, but only Y% on arwiki"; "edit spikes follow pageview spikes by N hours") | 2 h | **CP4** |
| S11 | Report: problem statement + dataset sections, results + insights (dashboard screenshots), limitations + extensions | 5 h | **CP5** |
| S12 | Slides: problem + dataset (~2 min), dashboard + insights (~4 min) | 1.5 h | **CP5** |
| S13 | **Fresh-clone test** on her own laptop following only the README (sample mode + live mode); log every step that was unclear | 1 h | **CP6** |
| S14 | Q&A prep sheet: ~15 likely questions with short answers (why Kafka and not polling? why DuckDB + Postgres? why dbt not Spark? what happens if the collector stops? how is it scaled?) | 1.5 h | **CP6** |

**Dependencies and fallbacks:**
- **L5 needs S2.** If S2 is late, Lynn wires the DAG with a stub that downloads one fixed hour.
- **L8 needs S3.** If S3 is late, `dim_wiki` starts from the distinct wiki ids in the stream.
- **S9 needs L8's marts (CP3).** Until then, Sumaiya builds the tiles on the CP2 thin-slice mart and repoints them at CP3.

### Both
| ID | Task | Due |
|---|---|---|
| T1 | Read this file + DELIVERABLES.md; agree on the problem statement wording (L3) | **CP1** |
| T2 | Read the whole report draft and comment on every section | **CP6** |
| T3 | Rubric self-check against DELIVERABLES.md: Sumaiya on Problem / Dashboard / Documentation / Presentation, Lynn on Ingestion / Warehouse / Transformations / Reproducibility | **CP6** |
| T4 | Both can explain the **whole** architecture (Q&A questions can go to either of us) | Rehearsals 19 + 23 Nov |

---

## Checkpoint checklists

**CP0 · Mon 26 Oct (in the chat, by 10 pm):** L1 · S1. Each person posts `docker ps` with the Kafka broker up.
**Collector · Tue 27 Oct:** L2. Lynn posts the first hour's file and its event count.
**CP1 · Thu 29 Oct:** L3 · S2 S3 S5 · T1
**CP2 · Fri 6 Nov:** L4 L5 L6 · S6 (v1) S7 (thin slice end to end)
**CP3 · Tue 10 Nov:** L7 L8 L9 (pipeline frozen)
**CP4 · Fri 13 Nov:** L10 L11 · S6 (final) S8 S9 S10 (window frozen, dashboard final)
**CP5 · Mon 16 Nov:** L12 L13 · S11 S12 (full drafts)
**CP6 · Thu 19 Nov:** S13 S14 · T2 T3 · rehearsal 1 (95% done)

If a task slips, say so at the checkpoint and **cut scope rather than move the date**, in this order:
1. Article topics from Wikidata (stretch goal only, not in the plan above)
2. The daily Parquet merge task in L9
3. The dead-letter topic in L7
4. Dashboard tile 5 (completeness): keep the numbers in the report table instead

**Never cut** the processor + split topics + consumer group (L7), the dbt tests (L8) or sample-data mode
(L10). These are what move rows from "borderline" to "good".
