# Q&A prep sheet (S14)

45 questions the instructor or other students are most likely to ask, grouped by the rubric row each one
tests. Questions 21–45 are the hard ones from three rounds of a strict mock review (23 Nov): several expose real
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
  edits but #65 by human edits (99.5% bots). Humans peak at 17:00 UTC, 2.1× the 04:00 low (13:00–19:00 UTC: the European afternoon and American
  morning).
- Attention spikes are *not* an early sign of edit surges: 77.5% of 24,234 spikes get no edit, and at
  every level of views a spike is followed by an edit less often than a steadily popular article (above
  20,000 views: 57% against 84% within two hours). Every number in section 6: `analysis/findings.sql`.

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
successful build the marts are copied into Postgres, all in a single transaction, and Metabase reads those
copies through a read-only user.

**14. Explain your schema. How is it optimised for the dashboard?**
- A star schema: one fact table at the finest grain (one row per edit), plus dimensions for time, wiki,
  article and editor type, and two pageview fact tables.
- Facts are incremental: each run reloads only the last three days.
- Four pre-aggregated marts, each serving one or more dashboard tiles, so tiles don't scan millions of edits.
- Marts store counts, not shares, so any date range can be summed before dividing.

**15. Why dbt and not plain SQL scripts or Spark?**
dbt keeps the SQL under version control, works out the build order from the model dependencies, and runs
35 data tests on every build. If any test fails, the export doesn't run, so the dashboard never shows bad
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
each upload queues a `warehouse_hourly` run. It works, but takes hours. To check the batch path, leave it paused and
run one recent hour with `airflow dags test pageviews_hourly <yesterday's hour>`; the README gives the exact
command. Hours older than the 3-day lookback need one dbt run with a longer `lookback_days`.

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
built. The gate worked: the export was skipped and the dashboard kept the last good data. Fixed 23 Nov:
`fact_edit`, `dim_article` and `dim_wiki` all stop at the hour the dbt run started (macro
`before_run_start`), so the hour still being written is left out of every model and loaded by the next
run. A full build afterwards passed all 51 nodes and left the window's 25,093,717 edits unchanged.

**32. Can I reproduce your five findings from the repo?**
With our collected data, yes: `analysis/findings.sql` holds the query behind every number in section 6, run
against the exported marts in Postgres, and reproduces them exactly. The 15-day window itself (25M edits) is
not in the repo; a fresh clone has only the six sample hours, so the same queries run but give sample numbers. Findings 1, 2, 4 and 5 can also be read off the dashboard tiles.

## Dashboard

**33. Finding 5 answers part 3 of the question. Where on the dashboard does a patroller see it?**
On the tile *Do attention spikes bring edits?*: the share of article-hours with at least one edit in
the next two hours, by views in the hour, spikes against steadily popular articles. Spikes are lower in
every band: 14% vs 20%, 32% vs 48%, 57% vs 84%. The table tile under it lists the largest spikes and the
edits after them, so a patroller can still see the big news stories.

**34. What is your comparison group, and is the difference more than noise?**
Article-hours with as many views that are *not* spikes (under 5× their previous 24-hour average), on
articles only. It's descriptive: no significance test, and the same story often spikes several hours in a
row, so the observations aren't independent. Above 20,000 views the comparison group is only 39 articles
(580 hours), so a few long-running stories dominate it. But the direction is the same in all three view
bands, including 1,000–5,000 views with 21,472 spikes and 64,746 other hours, so it isn't a small-sample
accident. The next step would be a matched comparison with confidence intervals.

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
40.9% of human edits. So the peak window, 13:00–19:00 UTC, is the European afternoon *and* the American
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

## Third review round (23 Nov)

**41. What pages were in your comparison group, and why did finding 5 change?** *(Sumaiya)*
Our first comparison kept every page in the pageview files. Above 20,000 views, 1,746 of the 2,326
non-spike hours were main pages, *Special:Search* and *Wikipedia:* project pages, which get huge views and
are almost never edited (0.9% of those hours had an edit). That made spikes look three times as edited.
We now ask each Wikipedia's own API for its namespaces and main page (a dbt seed) and keep articles only;
then steadily popular articles are edited *more* often than spiking ones (84% vs 57% of hours). The
mistake and the fix are in the report's incident table.

**42. Every `pageviews_hourly` run is green, yet 13 hours are missing. How? Why not reload them?** *(Lynn)*
A run's state follows its last task, and the clean-up step, which runs even after a failure, was last and
succeeded, so failed hours looked green and were never retried. They failed within a second, while the
laptop was offline. Fixed: clean-up is now a teardown task, which Airflow ignores for the run's state, and
a network error counts as "file not published yet". We found it on 23 Nov, after the numbers were frozen;
the files are still online, so clearing those 13 runs would load them, and we will after grading. We
didn't before submission because every spike baseline in the 24 hours after each one would change after
the report was written. The effect is bounded: 13 of 360 hours, and Q35 quantifies the baseline error.

**43. `export_marts` replaces four tables. Can Metabase see two different builds at once?** *(Lynn)*
Not any more: until 23 Nov each table had its own transaction, so for a few seconds one tile could show
the new build and another the old. Now all four are dropped and recreated in one transaction; a dashboard
query that arrives meanwhile waits for the commit and then sees the new build everywhere.

**44. You fetch file HH+1 for the hour HH:00–HH+1:00. How do you know, and what breaks if it's off by one?**
*(Lynn)*
We checked one file against Wikimedia's Pageview API on 26 Oct: the file named 12:00 has exactly the API's
11:00–12:00 numbers for English Wikipedia (`ingest/pageviews.py`). If we were off by one, every spike would
be shifted an hour against the edits, moving edits between "same hour" and "next two hours".

**45. Finding 5 reversed a day before submission. Why should we trust the other four?** *(Sumaiya)*
Findings 1–4 don't use the pageviews: they count edits, where each edit has its own revision id (zero
repeats among 25,093,717) and the bot flag comes with the event. The error in finding 5 was in which pages
we compared, not in the pipeline, and the reversal is shown in the report rather than hidden. Every number
in section 6 is reproduced by `analysis/findings.sql` on the serving database (with our collected data;
the window itself is not in the repo).

## Fourth review round (23 Nov, harsh mock instructor)

**46. Your question is about Wikipedias. Why is a headline 48.6%, across all wikis?** *(Sumaiya)*
The Wikipedia figure is 45.3%, and that's the one the slides lead with. 48.6% covers all 771 wikis,
including Wikidata and Commons (52.4% of all edits, mostly bot-driven structured data and file uploads).
We collect every wiki because the feed is one stream and filtering is cheaper downstream; the "about 1.8×"
overstatement (1 / (1 − 0.453)) is the Wikipedia number.

**47. The 77.5% counts the spike hour plus two hours, but the bars count only the next two. Why switch?** *(Sumaiya)*
They answer different questions. 77.5% is the broad one: did anything at all happen in the spike hour or
the two after? The bars compare spikes with steadily popular articles, and there the spike hour is left out
because an edit made before the jump in the same hour can't be a response to it. Both windows are stated
in the report (section 6, finding 5). Counting the spike hour too doesn't change the direction: 20% against
26%, 42% against 57%, 68% against 90% (checked 23 Nov on the serving database).

**48. Steadily popular articles above 20,000 views are 39 ongoing-news pages that are always edited. Isn't the comparison confounded?** *(Sumaiya)*
Partly, yes. The comparison is matched on views only, not on topic or article age, so it's descriptive;
the report says so in the limitations, along with the fact that consecutive spike hours of one story aren't
independent. The direction holds in all three view bands, including 1,000–5,000, with 21,472 spike hours
against 64,746 steady ones. A matched comparison (same topic and age) is the next step.

**49. You say the batch path runs end to end, but the seeds are made by a script you run, and the dead-letter topic is read by hand.** *(Lynn)*
The recurring work runs in Airflow: every pageview hour and every warehouse build, with the quality gate.
The two seeds are reference data that changes rarely (wiki names, non-article titles); they're generated by
scripts in `ingest/` and committed, so a fresh clone needs no manual step. The dead-letter topic is for
inspection by design: one event in three weeks.

**50. Did you check 48.6% against Wikimedia's own statistics?** *(Sumaiya)*
Not formally. Wikistats reports edits by editor type per wiki per month, so a cross-check is possible but
covers monthly periods, and we didn't do it. Our number comes straight from the `bot` flag on each event. We say the bot share is an estimate because unflagged bots count as humans.

**51. Who built what?**
Sumaiya: the problem framing, the architecture diagram, the dashboard tiles and their SQL, the findings, and
report sections 1, 2, 6 and 7. Lynn: the pipeline code (stream, batch, dbt, Docker, scripts, tests) and
report sections 3–5, and assembling the report.

**52. At-least-once: what if a Kafka send fails?** *(Lynn)*
Since 23 Nov every send is checked: the producer and processor attach an error callback to each send, and
after flushing they refuse to commit offsets or save the stream position if any send failed; the service
exits and Docker restarts it from the last commit. Before that, `flush()` doesn't raise on a failed send,
so a lost event was possible in principle (none were seen: the hourly counts have no gaps).

