# Q&A prep sheet (S14)

40 questions the instructor or other students are most likely to ask, grouped by the rubric row each one
tests. Questions 21–40 are the hard ones from two rounds of a strict mock review (23 Nov): several expose real
weaknesses, and the answers say how to admit them honestly. The first sentence of each answer *is* the answer; say the rest only if they follow up (about 20–30
seconds each). Both of us should be able to give every answer. All numbers are from the analysis window,
28 Oct – 11 Nov 2026 (UTC), as in the report.

---

## Problem description

**1. Why does this question matter, and who is it for?**
Wikipedia's volunteer patrollers review recent edits for vandalism and mistakes, and there are about 1.7
million edits a day. The dashboard tells them which wikis are busiest, how much of the volume is flagged
bot work, which is hidden from their default review list, and whether a jump in readers warns of a rush of edits.

**2. So does reader attention *cause* edits?**
We don't claim that: we show whether attention spikes are *followed by* edit surges. A news event can
drive both, so we can show timing but not cause.

**3. What did you find?** *(answer with the dashboard open)*
- How much is bots: about half, 48.6% of all edits and 45.3% on Wikipedias, so raw edit counts overstate
  what patrollers see by about 1.8× on Wikipedias.
- Where editing pressure concentrates: English Wikipedia alone has 40.9% of human Wikipedia edits, and
  the top 10 of 347 Wikipedias have 74.2%. Ranking by total edits misleads: Chechen Wikipedia is #2 by
  edits but #65 by human edits (99.5% bots). Humans peak at 17:00 UTC, 2.1× the 04:00 low (13:00–18:00 UTC: the European afternoon and American
  morning).
- Attention spikes: most are *not* followed by edits (77.6% of 24,325 get none). Only the largest ones
  are: above 20,000 views, spikes get 3.1× the edits of non-spike article-hours above 20,000 views in the next
  two hours (57% get at least one edit, against 22%). Every number in section 6: `analysis/findings.sql`.

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

---

# Hard questions (strict mock review, 23 Nov)

## Problem description

**21. Patrollers mostly work inside one wiki. Who actually acts on a cross-wiki ranking?**
Cross-wiki patrollers do: stewards, global sysops and small-wiki monitoring volunteers watch many small
wikis at once, and they are the ones who need to know that Chechen's volume is almost all bots. Within one
wiki, the hourly profile and the spike tile are the useful parts. *Honestly:* we didn't validate this with
patrollers; that would be the first next step.

**22. Bots make mistakes too. Isn't "half the edits are work they don't need to check" overstated?**
Read literally, yes, which is why the report says *hidden from their default review list*: flagged bot
edits are hidden from the default recent-changes view, so raw counts overstate what patrollers actually
see by about 1.8× on Wikipedias. The share is an estimate either way: unflagged bots count as humans, and
a flagged account's manual edits count as bots.

## Stream ingestion

**23. Why key `wiki.raw` by wiki? Which consumer needs per-wiki order? Aren't the 3 partitions skewed,
with Wikidata and Commons over half the volume?**
No consumer needs the order: the minute counts use event time, and the sinks file events by event-time
hour. The reason is co-location: the per-wiki minute counts are state, and keying by wiki keeps each
wiki's state in one processor instance if we scale it. The cost is skew, and we accept it at 20 events/s;
at higher volume we'd salt the hot keys or key the sink topics by event id.

**24. You say at-least-once everywhere. What guarantee does the live tile have?**
A weaker one. The processor commits offsets after flushing its outputs, but minutes that are still open
live only in memory, so a restart can under-count them; and live-stats adds counts into Postgres, so a
crash between writing and committing can double-count a minute. That tile is approximate by design; the
warehouse is the source of truth. A real fix needs checkpointed state (Flink, Kafka Streams).

**25. What if the laptop is off for longer than Wikimedia keeps its replay history?**
That data is gone: EventStreams has no long-term archive, and `?since=` only reaches back as far as
Wikimedia keeps events (days, not weeks). The gap would show on the completeness tile and in
`stream.status`; nothing is filled silently. Our longest gap was 16 hours, well inside that. This is
exactly why the first extension is a collector on a server.

## Batch ingestion and orchestration

**26. If I clone this and unpause `pageviews_hourly`, what happens?**
Catchup runs every hour since 25 Oct 05:00: about 740 runs and 42 GB of downloads, one at a time, and
each upload queues a `warehouse_hourly` run. It works, but takes hours; to check the batch path, trigger
one run. The README says this.

**27. Your raw pageview zone is already filtered to ≥ 10 views. Doesn't that break your own rule that
the raw zone keeps everything?**
Yes, for pageviews: the raw zone is lossless only for edits. The reason is that the pageview files stay
online, unchanged and CC0, so Wikimedia's server is effectively our raw layer, while 57 MB × 360 hours
(~20 GB) is more than a laptop can spare. Changing the threshold means downloading again, which the
idempotent DAG handles.

## Data warehouse

**28. The marts "store counts so any date range can be summed". Can I sum `editors`?**
No. `editors` is a distinct count, and distinct counts don't add up: summing hours counts a person once
per hour. No tile sums it. The rule holds for the event counts (edits, new pages, reverts). To do
distinct editors over any range, compute from `fact_edit` or store HyperLogLog sketches.

**29. Why isn't `fact_pageviews_hourly` linked to `dim_time`, and why no uniqueness test on its grain?**
An omission. `dim_time` ends at the last *edit* hour, so a pageview hour after it would fail the test.
The fix is to extend the time spine to the latest hour of either source and add a unique-combination test
on hour × wiki × article × access. The incremental `unique_key` already deduplicates; nothing tests it.

**30. At 10× the volume, or a year of data, what breaks first?**
In order: the hourly full rebuild of the marts over the whole fact table (DuckDB is capped at 1.5 GB and
one thread); `dim_article`'s `NOT IN` against millions of keys; then the single Kafka broker. The path is
incremental marts by hour and facts partitioned by date, then a bigger engine (ClickHouse, or DuckDB on a
larger machine) and 3 brokers. The lake layout doesn't change.

## Transformations

**31. The relationship test failed four times on 8–10 Nov. What was the cause?**
Most likely consistency: `dim_article` and `fact_edit` both read the live clean zone, and the sinks write
new files every few minutes, so a fact row can point to an article written after `dim_article` was
built. The gate worked: the export was skipped and the dashboard kept the last good data. The real fix is
for each run to read only hours before its start time.

**32. Can I reproduce your five findings from the repo?**
Yes: `analysis/findings.sql` holds the query behind every number in section 6, run against the exported
marts in Postgres; it reproduces them exactly. Findings 1, 2, 4 and 5 can also be read off the dashboard tiles.

## Dashboard

**33. Finding 5 answers part 3 of the question. Where on the dashboard does a patroller see it?**
On the tile *Do attention spikes bring edits?*: the share of article-hours with at least one edit in
the next two hours, by views in the hour, spikes against other article-hours. Below 20,000 views spikes
do no better (14% vs 18%, 32% vs 33%); above it, 57% vs 22%. The table tile under it lists the largest
spikes and the edits after them.

**34. What is your comparison group, and is 3.1× more than noise?**
Article-hours above 20,000 views that are *not* spikes (under 5× their previous 24-hour average). It's
descriptive: 346 spikes over 15 days, no significance test, and the same story often spikes several hours
in a row, so the observations aren't independent; a news event can also drive both views and edits. The
next step would be a matched comparison with confidence intervals.

**35. How sound is the spike baseline? Hours under 10 views and hours with no pageview file both count
as zero.**
Under-10 hours barely matter: the average is off by under 10 views against a 1,000-view threshold. The 13
missing hours matter a little more: the baseline divides by 24 regardless, so for a day after a missing
hour an article's baseline is up to ~4% too low and spikes look slightly larger. That's small next to the
5× rule, and it's stated in the report's limitations; the fix is to divide by the hours actually present.

## More hard questions (second review round)

**36. Patrick Stewart had 1,006,226 views in one hour. How do you know a spike isn't automated traffic?**
The hourly pageview files count human readers only: Wikimedia has already removed known crawlers and the
traffic it detects as automated. *Honestly:* that filter isn't perfect and we didn't validate individual
spikes. This one is believable: it got 121 edits in the same hour. The next step would be to cross-check
spikes against Wikimedia's separate count of automated traffic.

**37. Is "European afternoon" really what 17:00 UTC means?**
Only partly: 17:00 UTC is 18:00 in Central Europe but noon in New York, and English Wikipedia alone has
40.9% of human edits. So the peak window, 13:00–18:00 UTC, is the European afternoon *and* the American
morning, which is how the report now puts it. We didn't split the hour profile by language;
`mart_edits_hourly` has `hour_of_day` and `language_name`, so that is one query away.

**38. You deduplicate by `event_id`. Could the same edit arrive twice with different ids?**
We checked: in the window, every one of the 25,093,717 edits has its own revision id per wiki, with zero
repeats. Each event is produced once by Wikimedia, and replays re-send the same id, so `event_id` is the
right key.

**39. A "partial" hour is under 50% of the median hour. Doesn't a normal night hour come close?**
Yes, closer than we'd like: the quietest normal hour in the window is 53% of the median. The four
flagged hours (28 Oct, 02:00–06:00) were far below, with bots at 6% and humans at 16% of the median at
03:00, and the raw zone confirms the feed itself was low. A sounder rule would compare each hour with the
median for the same hour of day; that's a one-line change in `mart_data_completeness`.

**40. 10.8 million duplicates is 30% of the raw zone. Why not avoid them at the producer?**
Replays deliberately overlap the edges of each gap so nothing is missed, and Kafka's idempotent producer
only stops repeats of the same send, not a second delivery from the source. Removing them later is cheap
and safe (compaction per hour, then by `event_id` in dbt), and the raw zone is meant to record what we
received. The cost is about 2.3 GB of extra compressed JSON (30% of the 7.52 GB raw zone); if storage mattered, tracking the last id per
gap would cut it.
