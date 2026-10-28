# Schedule, tasks and checkpoints (v2, 26 Oct)

**Project:** Wikipedia live edits pipeline (bots vs humans, edit pressure vs attention).
**Team:** Lynn Younes (lead) · Sumaiya

**Submission is due Tue 24 Nov** (GitHub repo + report PDF). Presentations are in the final sessions
**Presentation: Wed 25 Nov** (the day after submission; confirmed by Dr. Bouguelia on 28 Oct).

**Goal: 95% of the project done by Thu 19 Nov.** After 19 Nov we only polish, rehearse and submit.

## Where we are (26 Oct)

**The whole pipeline is built and running.** Live edits stream through Kafka into the MinIO lake
(since 25 Oct 05:58 UTC, no gaps), hourly pageviews are ingested by Airflow (backfilled from 25 Oct),
dbt builds the star schema every hour with 49 data tests, and Metabase shows the results. A fresh clone
works with one `docker compose up` (tested). All build tasks (L1–L11, S2, S3) are done; S2 and S3 were
taken over by Lynn on 26 Oct because they blocked the pipeline.

**What's left is analysis, writing and presenting.** Everything we report comes from the **analysis
window: Wed 28 Oct 00:00 UTC to Wed 11 Nov 23:59 UTC** (two full weeks). The window is frozen at CP4 so
the report numbers stop moving. All timestamps are **UTC**.

Effort estimates are hands-on time. **R:** = the rubric row a task earns marks on. The full deliverables
list is in [DELIVERABLES.md](DELIVERABLES.md); how everything works is in [docs/design.md](docs/design.md).

---

## Timeline

| Checkpoint | Date | Where the team should be | Status |
|---|---|---|---|
| Kick-off | Sun 25 Oct | Plan shared, repo up, collector live | ✅ |
| Pipeline built | Sun 25 – Mon 26 Oct | Stream, lake, Airflow, dbt star schema, dashboard, one-command setup, pageviews | ✅ (planned for 6–10 Nov) |
| **CP0** | **Tue 27 Oct** | **Sumaiya set up** (S1): repo cloned, stack running, dashboard open | |
| **CP1** | **Thu 29 Oct** | Problem statement agreed (L3, T1); presentation date + template known (S5) | S5 ✅ 28 Oct |
| Window opens | Wed 28 Oct 00:00 UTC | Analysis window starts (the collector just keeps running) | |
| **CP2** | **Fri 6 Nov** | Diagram v1 (S6) + tool table (S7) in the repo; dashboard tiles drafted (S9 v1) | |
| **CP3** | **Tue 10 Nov** | Pipeline frozen (only bug fixes); dashboard tiles reviewed together | |
| Window closes | Wed 11 Nov 23:59 UTC | Collector can stop running 24/7 | |
| **CP4** | **Fri 13 Nov** | **Window frozen**: dashboard final (S9), data profile (S8), insights with numbers (S10), diagram final (S6), sample regenerated from a window day | |
| **CP5** | **Mon 16 Nov** | **Full report draft** (every section) + **slide draft** | |
| **CP6** | **Thu 19 Nov** | **95% done**: report + slides reviewed by both, rubric self-check, Sumaiya's fresh-clone test, **rehearsal 1** (timed 15 min + mock Q&A) | |
| Polish | 20–23 Nov | Fix review comments only (Lynn's DSML presentation is Sun 22 Nov) | |
| Rehearsal 2 | Mon 23 Nov | Second timed run; **slides locked**; backup demo video recorded | |
| **Submit** | **Tue 24 Nov** | Repo link + report PDF submitted | |
| **Presentation** | **Wed 25 Nov** | ~15 min + 5 min Q&A, both speak | |

### Lynn's other deadlines (why the checkpoints sit where they do)
| Date | Other deadline |
|---|---|
| Wed 28 Oct · Fri 30 Oct · Sun 1 Nov | AICC CP4 · CP5 · CP6 |
| Thu 29 Oct · Wed 4 Nov | DSML CP2 · CP3 |
| Sun 8 Nov · Wed 11 Nov · Sat 14 Nov | DSML CP4 · CP5 · CP6 |
| Wed 18 · Fri 20 · Sun 22 Nov | DSML rehearsal 1 · rehearsal 2 · **presentation** |

**Daily update** in the group chat, every day by 10 pm (three lines, even if nothing changed):
```
✅ Done today:   (task IDs)
🚧 Blocked on:
➡️ Tomorrow:     (task IDs)
```
Lynn also posts the collector line (`docker compose exec raw-sink python -m stream.status`) until 11 Nov.
Blocked? Message **immediately**. Don't wait for the evening.

**Collector always-on period: until Wed 11 Nov 23:59 UTC** (Thu 12 Nov, 02:59 Qatar time). Lynn's laptop
stays plugged in, locked (never signed out) and online; sleep, hibernate and lid-close are off when
plugged in, Docker starts on sign-in, Windows Update is paused until 25 Nov. Unplugged trips are fine:
the producer replays what it missed after waking (a 4-hour outage on 25 Oct healed completely).

**Checkpoint call (15 min):** each person shows the tasks due, and we agree on the focus until the next
checkpoint. Done means finished, not started.

---

## Tasks

### Sumaiya: diagram, tool justification, dashboard, insights, half the report and slides
Owns `docs/diagram/`, `metabase/`, `paper/sections/01`, `02`, `06`, `07`.

| ID | Task | Effort | Due |
|---|---|---|---|
| S1 | **Setup.** Accept the GitHub invite, clone, Docker Desktop with **≥ 8 GB** memory, then just `docker compose up -d --build` (no `.env` needed). After ~3 min open the dashboard (http://localhost:3000, logins in the README). Post a screenshot of the dashboard + `docker compose ps` in the chat. Run it briefly only: Lynn's laptop is the one that collects; `docker compose down` when done | 30 min | **Tue 27 Oct** |
| S5 | ✅ **Asked 28 Oct (Lynn)**: presentation **Wed 25 Nov**; any template, references + appendices **don't count** towards the 10 pages; repo public or invited, our choice (we make it public at submission); live demo optional; dbt, DuckDB, Metabase are fine; **one submission per group**. The idea got positive feedback. Not asked yet: whether an AI-use statement is required | | |
| S6 | **Architecture diagram** (draw.io → `docs/diagram/architecture.svg` + `.png` + `.pdf` for the report): stream path, batch path, lake zones, warehouse, serving. Everything it needs is in [docs/design.md](docs/design.md) §3–§5 and the README's text diagram. v1 at CP2, final at CP4 [R: Documentation: "with diagram"] | 2 h | **CP2** / **CP4** |
| S7 | **Tool justification table** for the report: per tool, its job, 1–2 alternatives, why we picked it; stress the ones not covered in class (DuckDB, dbt, Metabase) and the MinIO image problem (design.md §5). Put it in `paper/sections/04_design_choices.tex` where the `\todo` marks it (PR) [R: "explain tools not covered in the course"] | 2 h | **CP2** |
| S8 | **Data profile** for the report, from the marts over the window: events per day, bot share, duplicates removed, gap/partial hours (`mart_data_completeness`), storage raw vs Parquet → one table in `02_dataset.tex` | 2 h | **CP4** |
| S9 | **Metabase dashboard** "Wikipedia edits". Two tiles exist (live per-minute, edits per hour bots vs humans). Add: (1) bot share by wiki, top 15 (`mart_wiki_activity`); (2) top languages by edits, Wikipedias only; (3) attention vs edits: biggest spikes and their edits (`mart_attention_vs_edits`, `is_spike`); (4) data completeness by hour. Each tile: title + one-line explanation; filters: date range, project. v1 at CP2, final at CP4. When final, add each tile's SQL to `CARDS` in [scripts/metabase_setup.py](scripts/metabase_setup.py) (or send it to Lynn) so a fresh install recreates it [R: Dashboard] | 4 h | **CP2** / **CP4** |
| S10 | **Insights:** 3–5 findings with numbers from the window (e.g. "X% of edits are bots, but only Y% on arwiki"; "Chechen Wikipedia is #2 by edits: who makes them?"; "articles with an attention spike get N× more edits within 2 hours") | 2 h | **CP4** |
| S11 | **Report:** `01_introduction`, `02_dataset` (+ S8 table), `06_results` (dashboard screenshots + S10), `07_discussion` (limitations + extensions). Each file has `\todo` notes on what goes there | 5 h | **CP5** |
| S12 | Slides: problem + dataset (~2 min), dashboard + insights (~4 min) | 1.5 h | **CP5** |
| S13 | **Fresh-clone test** on her Mac following only the README; log every step that was unclear | 1 h | **CP6** |
| S14 | Q&A prep sheet: ~15 likely questions with short answers (start from the list in [docs/demo.md](docs/demo.md)) | 1.5 h | **CP6** |

### Lynn (lead): architecture, all pipeline code, integration, report assembly
Owns everything else: `docker-compose.yml`, `stream/`, `ingest/`, `airflow/`, `dbt/`, `scripts/`, `postgres/`, `sample/`, `tests/`, `README.md`, `docs/design.md`, `paper/main.tex` + `paper/sections/03`–`05` (+ the Overleaf project).

| ID | Task | Status / effort | Due |
|---|---|---|---|
| L1–L2 | Repo + layout; collector live since 25 Oct 05:58 UTC | ✅ 25 Oct | |
| L4–L9 | MinIO lake; Airflow; dbt thin slice; stream processor + consumer group + live tile; star schema (49 tests); `warehouse_hourly` DAG | ✅ 25 Oct | |
| L10–L11 | One-command setup + sample mode (fresh-clone tested); README v1 | ✅ 25 Oct | |
| S2, S3 | Pageview ingestion (verified against the Pageview API) + wiki names seed; pageviews switched on and backfilled | ✅ 26 Oct | |
| L16 | Paper skeleton + Overleaf project | ✅ 25 Oct | |
| L3 | Design note ✅ written; **agree the problem statement with Sumaiya** (T1) | 15 min | **CP1** |
| L17 | Collector check every evening; keep the laptop plugged in until 11 Nov | 2 min/day | until 11 Nov |
| L18 | Review and merge Sumaiya's PRs (diagram, tool table, report sections); add her final tiles to `metabase_setup.py` if she sends the SQL | as they come | |
| L19 | After the window closes: regenerate the sample from a window day (`scripts/make_sample.py`), add the diagram to the README | 30 min | **CP4** |
| L12 | Report sections 03–05: ✅ drafted; add S6 diagram, S7 table, dbt lineage figure, final numbers from the window | 3 h | **CP5** |
| L13 | Slides: template, architecture, pipeline walkthrough, design decisions + incidents (~7 min); assemble the deck | 2 h | **CP5** |
| L14 | Demo script ✅ ([docs/demo.md](docs/demo.md)); record the backup video | 30 min | Rehearsal 2 |
| L15 | Final package: **make the repo public** (check it has no secrets: `.env` is git-ignored), report PDF, one submission for the group | 1 h | **Tue 24 Nov** |

### Both
| ID | Task | Due |
|---|---|---|
| T1 | Read this file + DELIVERABLES.md + the README; agree on the problem statement wording (docs/design.md §1) | **CP1** |
| T2 | Read the whole report draft and comment on every section | **CP6** |
| T3 | Rubric self-check against DELIVERABLES.md: Sumaiya on Problem / Dashboard / Documentation / Presentation, Lynn on Ingestion / Warehouse / Transformations / Reproducibility | **CP6** |
| T4 | Both can explain the **whole** architecture (Q&A questions can go to either of us) | Rehearsals 19 + 23 Nov |

---

## Checkpoint checklists

**CP0 · Tue 27 Oct (in the chat):** S1 (dashboard screenshot + `docker compose ps`).
**CP1 · Thu 29 Oct:** L3 · S5 ✅ · T1
**CP2 · Fri 6 Nov:** S6 (v1) · S7 · S9 (v1 tiles)
**CP3 · Tue 10 Nov:** pipeline frozen · dashboard tiles reviewed together
**CP4 · Fri 13 Nov:** L19 · S6 (final) · S8 · S9 (final) · S10 (window frozen)
**CP5 · Mon 16 Nov:** L12 L13 · S11 S12 (full drafts)
**CP6 · Thu 19 Nov:** S13 S14 · T2 T3 · rehearsal 1 (95% done)

If a task slips, say so at the checkpoint and **cut scope rather than move the date**, in this order:
1. Dashboard tile 4 (completeness): keep the numbers in the report's data profile instead
2. The dbt lineage figure in the report (describe the layers in a table instead)
3. Sample regeneration (the 25 Oct sample works; it's just outside the window)

**Never cut** the dashboard's spike tile (it answers a third of the research question) or the
fresh-clone test (reproducibility is a rubric row).
