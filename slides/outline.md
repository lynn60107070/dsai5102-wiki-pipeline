# Presentation outline (skeleton)

**Wed 25 Nov · 15 min talk + 5 min Q&A · 2 speakers, one handover**

This is the skeleton we each build our HTML slides from. Every slide lists what goes on it, what to
say, the numbers, and **what the instructor is listening for** (the rubric's "Good" column, read
strictly). Numbers come from the report (analysis window 28 Oct – 11 Nov 2026, UTC); don't put a number
on a slide that isn't in the report. Q&A answers are in [docs/qa.md](../docs/qa.md).

---

## How the instructor will grade the talk

The brief says the talk must: (1) describe the problem and dataset, (2) explain the architecture and
tools, (3) demonstrate the pipeline, (4) show the dashboard, (5) discuss challenges, design decisions and
lessons, (6) answer questions. Grading is holistic over 9 rubric rows. A harsh but fair grader will mark
us down for:

- **A vague problem.** Who is it for, what decision does it support? (Problem Description)
- **Tools without reasons.** Every tool needs a job *and* why it beat an alternative, especially the
  ones not taught: DuckDB, dbt, Metabase. (Documentation, Presentation)
- **A 1 producer → 1 consumer stream.** We must *show* several topics, a consumer group and the
  dead-letter topic. (Stream ingestion)
- **Partial orchestration.** Show that every batch step runs in the DAG, nothing by hand. (Batch)
- **Tables that aren't modelled.** Name the grain, the star, and why marts exist. (Warehouse)
- **"Basic SQL".** Show dbt layers, incremental models and tests as a gate. (Transformations)
- **Charts without a message.** Each finding = one number + what it means for patrollers. (Dashboard)
- **"It works on my machine".** Show one command on a fresh clone. (Reproducibility)
- **Reading slides, no ownership, shaky answers.** Both of us must be able to answer anything.
  (Presentation)

**Rubric coverage map**

| Rubric row | Slides |
|---|---|
| Problem Description | 2, 3 |
| Ingestion: stream | 9, 10 |
| Ingestion: batch / orchestration | 11 |
| Warehouse | 12 |
| Transformations | 12 |
| Dashboard / Visualization | 5, 6, 7, 13 (demo) |
| Reproducibility | 13+14 (end of demo) |
| Documentation (diagram, tool reasons) | 8, 9 |
| Presentation & Communication | all; 15, 16 (challenges, lessons) |

---

## The order: the brief's six items, three handovers

The deck (`slides/deck/index.html`) follows the brief's list in order: (1) problem and dataset, (2)
architecture and tools, (3) demonstrate the pipeline, (4) dashboard and insights, (5) challenges, design
decisions and lessons, (6) questions. Slide 2 is an agenda with exactly these six items, every slide's
marker names its item ("03 The pipeline · Stream ingestion"), and a chip in its top-right corner names
the rubric row it earns. Each of us presents our own content, so the talk hands over three times:

- **Sumaiya (2:40):** title, agenda, problem, data. *Handover:* "Lynn will show you how the pipeline turns
  these three sources into answers."
- **Lynn (6:50):** architecture, tools, stream, batch, warehouse and dbt, live demo ending on the
  dashboard. *Handover (on the dashboard):* "Sumaiya will show you what it tells us."
- **Sumaiya (2:50):** dashboard, findings 1–5. *Handover:* "Lynn will close with what broke along the way
  and what we learned."
- **Lynn (1:20):** challenges and design decisions, lessons and the answer, questions.

**Target 13:40 spoken.** Section numbers below are the old outline's; the deck numbers are in the table.

| Deck # | Slide | Brief item | Speaker | Time |
|---|---|---|---|---|
| 1 | Title | | Sumaiya | 0:15 |
| 2 | Agenda (the brief's six items) | | Sumaiya | 0:10 |
| 3 | The problem | 1 Problem & dataset | Sumaiya | 1:00 |
| 4 | Data: three sources + profile | 1 Problem & dataset | Sumaiya | 1:15 |
| 5 | Architecture (3 builds) | 2 Architecture & tools | Lynn | 1:00 |
| 6 | Tools and why | 2 Architecture & tools | Lynn | 1:00 |
| 7 | Stream path: Kafka | 3 Pipeline | Lynn | 0:50 |
| 8 | Batch path: Airflow | 3 Pipeline | Lynn | 0:40 |
| 9 | Warehouse and dbt | 3 Pipeline | Lynn | 0:50 |
| 10 | Live demo + one command to reproduce | 3 Pipeline | Lynn | 2:30 |
| 11 | Dashboard overview | 4 Dashboard & insights | Sumaiya | 0:40 |
| 12–14 | Findings 1, 2–3, 4 | 4 Dashboard & insights | Sumaiya | 1:30 |
| 15 | Finding 5 | 4 Dashboard & insights | Sumaiya | 0:40 |
| 16 | Challenges and design decisions | 5 Challenges & lessons | Lynn | 0:50 |
| 17 | Lessons, limits and the answer | 5 Challenges & lessons | Lynn | 0:30 |
| 18 | Questions | 6 Questions | both | – |

Rehearse with a timer; if a run goes over 14:00, cut words, not slides.

Slide 13 is a demo, not a slide: switch to the terminal and browser. If it fails for more than ~20 s,
play the backup video (L14) without apologising.

---

## Block A · Sumaiya

### 1. Title (0:15)
- **On slide:** *Where does Wikipedia's editing pressure go? An end-to-end pipeline for live edits.*
  Both names + student IDs, DSAI5102, 25 Nov 2026, repo link (or QR code).
- **Say:** names, one-line hook: "Every edit on Wikipedia, live, for two weeks."

### 2. The problem: patrollers' limited time (1:00)
- **On slide:** the one-sentence question in large type: *Where does editing pressure concentrate across
  Wikipedia's language editions, how much of it comes from bots, and are spikes in reader attention
  followed by edit surges?* Below it, the three decisions it supports:
  1. Which wikis need review most?
  2. How much of the volume is flagged bot work, hidden from the default review list?
  3. Does a jump in readers warn of a rush of edits?
- **Say:** ~20 events/s, ~1.7 million edits a day; volunteer patrollers review recent changes for
  vandalism; their time is limited and the raw stream doesn't say where to spend it. Audience: patrollers
  and wiki admins.
- **Instructor listens for:** a named audience, a decision it supports, why it's non-trivial. Define
  "editing pressure" (edits per wiki over time that may need review).
- **Trap:** "Does attention *cause* edits?" → we show timing, not cause (Q&A 2).

### 3+4. Data: three Wikimedia sources + what we collected (1:15, one slide)
- **On slide:** a 3-row table: source · type · update rate · volume · joins on.
  - EventStreams *recentchange*: stream (SSE), continuous, ~20 kept events/s, 1.7 M/day.
  - Pageview dumps: batch files, hourly, ~57 MB and 6.4 M rows per file → we keep ~193,000 + per-wiki
    totals.
  - Sitematrix API: reference table, rarely changes, 1,073 wikis.
- **Say:** the sources arrive differently, **which is why the pipeline has a stream path and a batch
  path**. The stream can't be re-downloaded later, so the collector runs all the time; the pageview files
  stay online, so missed hours can be backfilled. Licences: pageviews CC0; stream and Sitematrix under the
  Wikimedia Terms of Use.
- **Instructor listens for:** volume, velocity, why both stream and batch (this sets up Block B).
- **Trap:** "Why drop pageviews under 10 views?" → a spike needs ≥ 1,000, so every spike is kept (Q&A 16).

#### Data profile strip (same slide)
- **On slide:** big numbers: **25,093,717 edits · 771 wikis · 15 days (360 hours) · 0 empty hours**.
  Smaller: 10.8 M duplicates removed (30% of the raw zone), 492 dead-letter events, raw 7.52 GB → clean
  1.08 GB (~7× smaller), pageviews for 347 of 360 hours.
- **Say:** window 28 Oct – 11 Nov, UTC. The only low hours (28 Oct, 02:00–06:00) are just as low in the
  raw zone, so the drop is in the feed, not a loss in our pipeline.
- **Instructor listens for:** that we checked our data, not just collected it.

### 5. Dashboard overview (0:40)
- **On slide:** the dashboard screenshot (`paper/figures/06_dashboard.png`), the 8 tiles labelled:
  live edits per minute · edits per hour (bots vs humans) · bot share of the 15 busiest wikis · top
  Wikipedia languages (bots vs humans) · edits by hour of day · spikes vs steadily popular articles · the
  largest spikes and the edits that follow · data completeness.
  Callout: From / To filter every warehouse tile; Project filters the two all-wiki tiles.
- **Say:** each tile maps to one part of the question; each has a title and a one-line explanation.
- **Instructor listens for:** ≥ 2 tiles (we have 8), one categorical and one over time, titles, labels,
  explanations.

### 6. Findings 1–3: bots and where pressure goes (1:10)
- **On slide:** three findings, each = one big number + one line of meaning.
  1. **About half is bots:** 48.6% of all edits, 45.3% on Wikipedias → raw counts overstate what
     patrollers see by ~1.8×.
  2. **Rankings by total edits mislead:** Chechen Wikipedia is #2 by edits, #65 by human edits (99.5%
     bots); Arabic #5 → #17. Small table: the 7 busiest Wikipedias, rank by edits vs rank by human edits.
  3. **Human pressure is concentrated:** English alone has 40.9% of human Wikipedia edits; the top 10 of
     347 have 74.2%.
- **Instructor listens for:** numbers tied to the decision ("rank by human edits, not total").

### 7. Findings 4–5: the clock and attention spikes (1:00)
- **On slide:**
  4. **Humans follow the clock, bots don't:** human peak 17:00 UTC is 2.1× the 04:00 low; 13:00–18:00
     carries 30.8% of the day → the European afternoon and American morning.
  5. **Attention spikes are not an early sign of edit surges:** 77.5% of 24,234 spikes get no edit in
     the spike hour or the two hours after, and at every level of views a spike is followed by an edit
     *less* often than a steadily popular article. Big news still can: *2026 Kraków shooting*, 203 edits
     in two hours.
- **Finding 5 as a chart, not text:** the dashboard tile *Do attention spikes bring edits?*: three view
  bands, spikes vs steadily popular articles, share with ≥ 1 edit in the next 2 h: 14% vs 20%, 32% vs
  48%, **57% vs 84%** (above 20k views: 346 spikes vs 580 hours of 39 articles). Articles only.
- **Say:** so an alert on jumps in views alone would mostly point at articles nobody then edits.
  **Handover line.**
- **Trap 1:** "What's in the comparison group?" → articles only; main pages and *Special:Search* were in
  it at first and made spikes look 3× as edited: we caught it and the finding reversed (Q&A 41).
- **Trap 2:** "Is it significant?" → descriptive; no significance test, consecutive hours of one story
  aren't independent, and only 39 articles above 20k; but the direction holds in all three bands (Q&A
  34). Queries: `analysis/findings.sql`.

---

## Block B · Lynn

### 8. Architecture (1:00)
- **On slide:** the architecture diagram (`paper/figures/architecture.pdf`), built up in **3 steps**
  (stream path → batch path → warehouse and serving) or with the other paths dimmed: the full diagram is
  too dense to read projected at once. Colour key: blue stream, green batch, brown reference.
- **Say:** two ingestion paths meet in one lake (MinIO, raw + clean zones), one warehouse models both
  (DuckDB + dbt star schema), served through Postgres to Metabase. 14 services, Docker Compose, one
  command.
- **Instructor listens for:** a clear end-to-end flow, lake → warehouse → dashboard, as the brief's
  steps describe.

### 9. Tools and why (1:00)
- **On slide:** a table: layer · tool · alternative considered · why. Mark the tools not covered in the
  course (DuckDB, dbt, Metabase).
  - Kafka vs RabbitMQ: replayable log, several independent consumers, consumer groups.
  - Python consumers vs Flink: ~20 events/s needs no cluster; unit-tested.
  - MinIO: S3-compatible, local, free.
  - Airflow vs cron: retries, backfill, data-aware scheduling.
  - DuckDB vs ClickHouse/BigQuery/Postgres: in-process, columnar, reads Parquet on S3.
  - dbt vs Spark/scripts: lineage, incremental models, tests as a gate.
  - Postgres for serving: DuckDB allows one writer.
  - Metabase vs Superset/Looker Studio: one container, rebuilt by script through its API.
- **Instructor listens for:** a *reason* for each tool, not a list. "The goal is not to use as many
  technologies as possible."
- **Trap:** "Why both DuckDB and Postgres?" (Q&A 13). "Why not Spark?" (Q&A 15).

### 10. Stream path: Kafka (0:50)
- **On slide:** topic diagram: producer → `wiki.raw` (3 partitions, keyed by wiki) → raw sink + processor
  → `edits.bot` / `edits.human` → **consumer group of 2 clean sinks**; `edits.per_minute` → live-stats;
  `wiki.deadletter`. 5 topics, 4 consumer groups.
- **Say:** at-least-once delivery (commit after write), duplicates removed by event id; event-time
  per-minute windows with a per-partition watermark; outages replayed with `?since=`.
- **Instructor listens for:** not 1 → 1: several producers/consumers, a consumer group, a dead-letter
  topic, a reason for partitions and keys.
- **Trap:** exactly-once? (Q&A 5). Late events? (Q&A 6). Laptop sleeps? (Q&A 7).

### 11. Batch path: Airflow (0:40)
- **On slide:** the two DAGs as boxes.
  - `pageviews_hourly`: sensor (file published) → filter → upload 2 Parquet files → clean-up; catchup
    backfills from 25 Oct; re-runs overwrite (idempotent).
  - `warehouse_hourly`: hourly **or** on the pageviews Dataset → compact lake → `dbt build` (with tests)
    → export to Postgres.
- **Instructor listens for:** end-to-end, multiple steps, upload to storage, nothing run by hand.
- **Trap:** "Why not cron?" (Q&A 10).

### 12. Warehouse and dbt (0:50)
- **On slide:** star schema: `fact_edit` (one row per edit), `fact_pageviews_hourly`,
  `fact_wiki_views_hourly`; dims time, wiki, article, editor type; 4 marts (one per tile). Next to it,
  the dbt lineage (`paper/figures/lineage.png`): staging → core → marts.
- **Say:** grain stated per table; incremental facts reload only the last 3 days; marts store counts,
  not shares, so any date range sums correctly; **35 data tests gate the export**: if one fails, the
  dashboard keeps the last good data (happened 4 times on 8–10 Nov).
- **Instructor listens for:** schema *justified* and optimised for the queries, transformations that
  scale and produce clean data.

### 13. Live demo (2:30), follow [docs/demo.md](../docs/demo.md)
1. Producer/processor logs scrolling: "every edit, live, through Kafka."
2. Lake via `mc ls`: raw zone and clean Parquet, `dt=` / `hour=` partitions.
3. Airflow: `warehouse_hourly` graph, a green run (optionally the dbt log `PASS`).
4. Metabase: the live tile, then the other tiles with **From = 28 Oct, To = 11 Nov** set beforehand,
   so the numbers match Sumaiya's slides.
5. End on the reproduce line below (slide 14's content, said over the dashboard or a one-line slide).
- **Instructor listens for:** the pipeline actually running, end to end.
- **Risk:** stack up 1–2 hours early, check the producer says "live from now" (not "backfilling"),
  hotspot ready. Details in docs/demo.md.

### 14. One command to reproduce (said at the end of the demo, ~0:20)
- **On slide:** `docker compose up -d --build` in large type. Underneath: a bootstrap service loads
  6 bundled hours of real edits (~380k) + 30 hours of pageviews, runs the warehouse DAG, builds the
  Metabase dashboard through its API, in ~5 min. Tested on fresh clones (macOS, Windows).
- **Instructor listens for:** clear, easy to run, it works.

### 15. Challenges and design decisions (0:50)
- **On slide:** 3–4 incidents, each = problem → fix (from the report's incident table):
  - laptop standby / dropped connections → automatic replay by time (`?since=`), resuming where it stopped
  - one partition raced ahead, 205,814 events wrongly dropped as late → per-partition watermark
  - sinks killed mid-backlog → 381,066 duplicates → compaction + 60 s graceful shutdown
  - Docker disk grew to 351 GB, warehouse writes lost → every lake file checked, warehouse rebuilt from
    the lake (why the raw zone exists)
  - (optional, if time) analysis check: main pages in finding 5's comparison group reversed the result →
    namespace seed from each wiki's API; Airflow runs that hid failed hours → teardown task
- **Instructor listens for:** honest problems and *design decisions that came from them*.

### 16. Lessons, limits and conclusion (0:30)
- **On slide:** two columns.
  - **Limits / next:** one laptop collects (→ a server), one Kafka broker (→ more), bot flag is an
    estimate, 15 days only.
  - **Answer:** about half the edits are bots; rank wikis by human edits; human pressure peaks
    13:00–18:00 UTC; jumps in readers are not an early sign of edit surges.
- **Say:** close on the question from slide 2.

### 17. Questions
- **On slide:** "Questions?" + repo link. Keep the architecture diagram one click away (backup slide).
- **Who answers:** the person whose block it is; the other adds only if asked. Both know everything in
  docs/qa.md. Expect the grader to ask Sumaiya a pipeline question on purpose: she should be ready for
  Q&A 23, 24, 31, 41 and 45, and Lynn for 21, 22, 34, 42 and 43.

---

## Backup slides (after 17, only if asked)
- Kafka topics and consumer groups table (report Table, appendix).
- Full incident table.
- dbt lineage at full size.
- Data profile table.

## Build notes for the HTML slides
- One idea per slide; findings as big numbers, not bullet walls.
- Same template and fonts for both blocks so it reads as one deck.
- Figures to reuse: `paper/figures/architecture.pdf`, `lineage.png`, `06_dashboard.png`,
  `docs/diagram/wikipedia_pipeline_architecture_v1.png`.
- Each slide's speaker script goes in its notes (Mon 23).
