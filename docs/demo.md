# Live demo script (≈ 4 minutes, task L14)

The live demo is optional per the brief, but the stream never goes quiet, so it is our strongest moment.
**Backup:** record this exact walkthrough the day before (Rehearsal 2, Mon 23 Nov) and keep the video
open in a tab. If anything misbehaves for more than ~20 seconds, switch to the video without apologising.

## 30 minutes before

```bash
docker compose up -d                                   # if it isn't running already
docker compose ps                                      # everything Up / healthy
docker compose exec raw-sink python -m stream.status   # "collector: running", no new gaps
```
- Airflow: `warehouse_hourly` last run green (trigger it once if the last run is older than an hour).
- Open these tabs in this order, logged in, zoomed so text is readable from the back of the room:
  1. Terminal with `docker compose logs -f --tail 5 producer processor` (font ≥ 18 pt)
  2. MinIO console http://localhost:9001 → bucket `lake`
  3. Airflow http://localhost:8080 → `warehouse_hourly` → Graph
  4. Metabase http://localhost:3000/dashboard/2#refresh=60
- Turn off notifications; laptop plugged in; Wi-Fi checked (the live tile needs it).

## The walkthrough

| # | Show | Say (one line each) |
|---|---|---|
| 1 | Terminal: producer + processor logs scrolling | "This is every edit on every Wikipedia, live: about 950 a minute, coming through Kafka right now." |
| 2 | Point at `last 60 s: N bot, M human, 0 dead-letter` | "The processor validates each event, drops user names, and splits bots from humans into separate topics." |
| 3 | MinIO: `lake/raw/edits/dt=…/hour=…` then `lake/clean/edits/…` | "Two zones in the lake: raw, exactly as received, and clean Parquet, partitioned by date and hour." |
| 4 | Airflow: `warehouse_hourly` graph, a green run | "Every hour, and whenever new pageviews land, Airflow compacts the lake, runs dbt with 38 data tests, and only if they pass, exports to Postgres." |
| 5 | (Optional) click a task → Logs → dbt `PASS=38` | "If a test fails, the export never runs, so the dashboard can't show bad numbers." |
| 6 | Metabase: live tile | "This tile is fed straight from the stream: this minute's edits appear about three minutes after it ends." |
| 7 | Metabase: the other tiles | Hand over to Sumaiya for the findings. |

## Likely live questions (short answers)

- **What if the laptop sleeps?** The producer resumes from the last event it saved; the completeness tile shows any real gaps.
- **Why both Kafka and Airflow?** Edits are an unbounded stream (Kafka); pageviews are hourly files (Airflow). Airflow never runs the stream.
- **Why Postgres if you have DuckDB?** DuckDB allows one writer; the dashboard reads copies in Postgres through a read-only user.
- **How does it scale?** `docker compose up -d --scale clean-sink=3`: Kafka rebalances the partitions across the consumer group.
