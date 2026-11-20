# Q&A prep sheet (S14)

20 questions the instructor or other students are most likely to ask, grouped by the rubric row each one
tests. The first sentence of each answer *is* the answer; say the rest only if they follow up (about 20–30
seconds each). Both of us should be able to give every answer. All numbers are from the analysis window,
28 Oct – 11 Nov 2026 (UTC), as in the report.

---

## Problem description

**1. Why does this question matter, and who is it for?**
Wikipedia's volunteer patrollers review recent edits for vandalism and mistakes, and there are about 1.7
million edits a day. The dashboard tells them which wikis are busiest, how much of the volume is flagged
bot work they don't need to check, and whether a jump in readers warns of a rush of edits.

**2. So does reader attention *cause* edits?**
We don't claim that: we show whether attention spikes are *followed by* edit surges. A news event can
drive both, so we can show timing but not cause.

**3. What did you find?** *(answer with the dashboard open)*
- How much is bots: about half, 48.6% of all edits and 45.3% on Wikipedias, so raw edit counts nearly
  double the patrollers' real workload.
- Where editing pressure concentrates: English Wikipedia alone has 40.9% of human Wikipedia edits, and
  the top 10 of 347 Wikipedias have 74.2%. Ranking by total edits misleads: Chechen Wikipedia is #2 by
  edits but #65 by human edits (99.5% bots). Humans peak at 17:00 UTC, 2.1× the 04:00 low.
- Attention spikes: most are *not* followed by edits (77.6% of 24,325 get none). Only the largest ones
  are: above 20,000 views, spikes get 3.1× the edits of steady articles of the same size in the next two
  hours (57% get at least one edit, against 22%).

---

## Stream ingestion (Kafka)

**4. Why Kafka? Why not write the stream straight to storage?**
Kafka keeps the events durably, so several consumers can read the same events independently and replay
them after a crash. The raw archive and the processor both read `wiki.raw` in their own consumer groups;
the processor writes bot and human edits to separate topics, which two clean sinks read in one consumer
group. Scaling the sinks is just adding containers: Kafka spreads the 3 partitions across them.

**5. Is delivery exactly-once? What about duplicates?**
At-least-once: each consumer commits its Kafka offset only after its output is safely written, so a crash
can repeat events but never lose them. Duplicates are removed by event id twice, by compaction in the lake
and again in dbt: 10.8 million in the window, 30% of the raw zone, mostly from replaying outages. Invalid
events go to a dead-letter topic with the reason instead of stopping the pipeline (492 in the whole run).

**6. How do the per-minute counts handle late events?**
They count by *event time*, not arrival time: a minute is emitted two minutes after it ends, so slightly
late events still count. We first used the newest event on any partition as the clock, and during a
backlog one partition raced ahead and 205,814 events were wrongly dropped as late. The fix is a
per-partition watermark: a minute closes only when every partition has passed it.

**7. What happens if the laptop sleeps or the network drops?**
The producer notices any gap over 3 seconds and queues the missing time on disk; a second connection
replays it from Wikimedia while live data keeps flowing. Every outage in the window was replayed (the
longest gaps were 9 and 16 hours), and no hour is empty. Replaying three days a second time added only
0.1% new edits.

---

## Batch ingestion and orchestration (Airflow)

**8. Why both Kafka and Airflow?**
The two sources arrive differently. Edits are an endless stream that can't be downloaded again later, so
they go through Kafka. Pageviews are hourly files that stay online, so a scheduled Airflow job fits, and
it can backfill missed hours. Airflow never runs the stream.

**9. Walk us through the DAGs.**
- `pageviews_hourly`, one run per UTC hour: a sensor waits until the file is published, then the file is
  filtered, two Parquet files are uploaded to the lake, and a clean-up step removes local copies. Catchup
  backfills every hour since 25 Oct; reruns overwrite the same files, so retries are safe.
- `warehouse_hourly` runs hourly *or* as soon as new pageviews land (an Airflow Dataset), so the two DAGs
  coordinate without calling each other: compact the lake → `dbt build` with tests → export to Postgres.

**10. Why Airflow and not cron?**
Cron only starts jobs. Airflow adds retries, backfill of missed hours, dependencies between steps, a view
of every run, and data-aware scheduling, which `warehouse_hourly` uses.

---

## Lake, warehouse and transformations

**11. Why a raw zone *and* a clean zone in the lake?**
The raw zone keeps every event exactly as received, so any cleaning mistake can be redone from it. The
clean zone is validated, typed Parquet without personal data, which DuckDB reads directly. Both are
partitioned by date and hour, so a query over a date range reads only those folders.

**12. Why DuckDB as the warehouse, and not Postgres, BigQuery or ClickHouse?**
DuckDB runs inside the process, so there's no server to operate. It's columnar, so analytical scans are
fast on a laptop, and it reads Parquet from MinIO directly. BigQuery is cloud-only and paid, ClickHouse is
another server, and Postgres is row-based and slower for large scans.

**13. Then why is there a Postgres too?**
A DuckDB file allows only one writer, so the dashboard would fail while dbt rebuilds it. After every
successful build the marts are copied into Postgres in one transaction each, and Metabase reads those
copies through a read-only user.

**14. Explain your schema. How is it optimised for the dashboard?**
- A star schema: one fact table at the finest grain (one row per edit), plus dimensions for time, wiki,
  article and editor type, and two pageview fact tables.
- Facts are incremental: each run reloads only the last three days.
- One pre-aggregated mart per dashboard tile, so tiles don't scan millions of edits.
- Marts store counts, not shares, so any date range can be summed before dividing.

**15. Why dbt and not plain SQL scripts or Spark?**
dbt keeps the SQL under version control, works out the build order from the model dependencies, and runs
34 data tests on every build. If any test fails, the export doesn't run, so the dashboard never shows bad
numbers; that happened four times on 8–10 Nov. Spark would need a cluster for data this size.

---

## Data decisions

**16. Why drop pageviews below 10 an hour? Aren't you losing data?**
One hourly file has 5.25 million Wikipedia rows, about 126 million a day, too much for a laptop warehouse.
Keeping ≥ 10 views leaves about 193,000 rows an hour. A spike needs at least 1,000 views and 5× the
previous 24-hour average, so every spike is still there, and per-wiki totals keep all views.

**17. How do you decide an edit is a bot's, and what about personal data?**
- Bots: from the bot flag, which each wiki's community grants to approved bot accounts. Unflagged bots
  count as humans and a flagged account's manual edits count as bots, so the share is an estimate.
- Personal data: the processor drops user names and edit summaries; the clean zone and warehouse keep
  only an MD5 hash to count distinct editors. The raw archive keeps events as received. If pushed: an
  unsalted hash of a public name can be guessed, so a salted hash is the next step.

---

## Reproducibility and lessons

**18. How would I run this myself, and what would you do differently?**
- Run it: `docker compose up -d --build` on a fresh clone. A bootstrap service loads six bundled hours of
  real edits (plus the 30 hours of pageviews the spike tile needs), runs the warehouse DAG once and sets
  up the Metabase dashboard through its API, in about 5 minutes. We tested this on fresh clones on macOS
  and Windows; the last test (20 Nov) found the spike tile failing without pageviews and a red first
  Airflow run, both fixed.
- Differently: collect on a server instead of a laptop (most of our incidents came from sleep and
  standby), use more than one Kafka broker, and detect reverts without relying on English words in edit
  summaries.

**19. How do you know the data is complete and correct?**
After the window closed we read back every lake file and checked it holds the number of events in its
name; the warehouse has exactly the clean zone's 25,093,717 edits, and all 360 hours have data. The only
low hours, 02:00–06:00 UTC on 28 Oct, are just as low in the raw zone, for bots and humans, so the drop
came from the feed, not from us. Checking the data also caught real bugs: the pageview files use the title
"-" for views of an unknown page, which showed up as 75 fake spikes until we filtered it.

**20. Can I see the data lake? Why is there no MinIO web page?**
MinIO stopped publishing free images in 2025; the last free build we pin has no web console. The lake is
browsed from the terminal instead: `docker compose exec minio mc ls --recursive local/lake/clean/edits/`
shows the date/hour partitions and Parquet files. DuckDB reads the same files directly over the S3 API.
