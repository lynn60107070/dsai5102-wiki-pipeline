# Live demo script (≈ 2:30, slide 10)

The live demo is optional per the brief, but the stream never goes quiet, so it is our strongest moment.
**Backup:** record this exact walkthrough before the presentation and keep the video open in a local
tab (no network needed). If anything misbehaves for more than ~20 seconds, switch to the video without apologising.

## After the window (done 23 Nov)

The collector stopped on 14 Nov (21:21 UTC). On restart the producer replays everything since its saved position,
which would be ~10 days and millions of events mid-demo. The saved position was moved to
`/state/archived-window/` in the `collector_state` volume, so the producer now starts **live only**.
Keep `pageviews_hourly` **paused** (on this laptop, unpausing it backfills every hour since 12 Nov).

## 1–2 hours before (stack up early)

```bash
docker compose up -d                                   # if it isn't running already
docker compose ps                                      # everything Up / healthy
docker compose logs --tail 20 producer                 # "live from now", no "backfilling since"
docker compose exec raw-sink python -m stream.status   # "collector: running"; the 14 Nov – today gap is expected
```
- Airflow: `warehouse_hourly` last run green (trigger it once if the last run is older than an hour).
- Metabase: set **From = 2026-10-28, To = 2026-11-11** before showing the findings tiles, so the numbers
  match the slides (new live hours would otherwise shift them). The live tile shows the last minutes.
- Test the room's Wi-Fi with the stream; keep a phone hotspot ready (some networks cut long-lived
  connections).
- Open these tabs in this order, logged in, zoomed so text is readable from the back of the room:
  1. Terminal with `docker compose logs -f --tail 5 producer processor` (font ≥ 18 pt)
  2. Terminal with `docker compose exec minio mc ls --recursive local/lake/clean/edits/dt=2026-11-04/` (MinIO has no web console)
  3. Airflow http://localhost:8080 → `warehouse_hourly` → Graph
  4. Metabase http://localhost:3000/dashboard/2#refresh=60
- Turn off notifications; laptop plugged in; Wi-Fi checked (the live tile needs it).

## The walkthrough

| # | Show | Say (one line each) |
|---|---|---|
| 1 | Terminal: producer + processor logs scrolling; point at `last 60 s: N bot, M human, 0 dead-letter` | "Every edit on every Wikimedia wiki, live: about 20 a second through Kafka. The processor validates each event, drops user names, and splits bots from humans." |
| 2 | Lake (`mc ls`): `lake/raw/edits/dt=…/hour=…` then `lake/clean/edits/…` | "Two zones in the lake: raw, exactly as received, and clean Parquet, partitioned by date and hour." |
| 3 | Airflow: `warehouse_hourly` graph, a green run | "Every hour, and whenever new pageviews land, Airflow compacts the lake, runs dbt with 35 data tests, and only if they pass, exports to Postgres. If a test fails, the dashboard keeps the last good data." |
| 4 | Metabase: live tile | "This tile is fed straight from the stream: this minute's edits appear about three minutes after it ends." |
| 5 | Metabase: the other tiles | Hand over to Sumaiya for the findings. |

## Likely live questions (short answers)

- **What if the laptop sleeps?** The producer sees the jump in time and replays the missing interval with `?since=` on a second connection; the completeness tile shows any real gaps.
- **Why both Kafka and Airflow?** Edits are an unbounded stream (Kafka); pageviews are hourly files (Airflow). Airflow never runs the stream.
- **Why Postgres if you have DuckDB?** DuckDB allows one writer; the dashboard reads copies in Postgres through a read-only user.
- **How does it scale?** `docker compose up -d --scale clean-sink=3`: Kafka rebalances the partitions across the consumer group.
