"""Collector status: events per hour in the lake, gaps, and whether files are still arriving.

Run: `docker compose exec raw-sink python -m stream.status` (add `--hours 72` to show more of the table).
The last line is the one-line status for the daily update. Counts come from the object names, so
nothing is downloaded.

Gaps are judged on the clean zone over the whole analysis window, not just the hours in the table: its
finished hours are compacted without duplicates, whereas in the raw zone a replay's repeats can make a
half-empty hour look full. An hour counts as a gap with no events and as low under half the typical hour,
so a smaller partial loss does not show here (7 Nov: "gaps: none" while hours held a third of their edits).
"""
import argparse
import os
from datetime import datetime, timedelta, timezone
from statistics import median

from stream import lake

STALE_AFTER = timedelta(seconds=2 * int(os.environ.get("FLUSH_SECONDS", "300")) + 60)
WINDOW_START = datetime(2026, 10, 28, tzinfo=timezone.utc)
LOW_SHARE = 0.5


def list_objects(s3, prefix: str, suffix: str) -> list:
    objects = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=lake.BUCKET, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return [o for o in objects if o["Key"].endswith(suffix)]


def hourly_counts(objects) -> dict:
    """{hour: (events, bytes)} from keys like <zone>/edits/dt=YYYY-MM-DD/hour=HH/part-...-n<events>.<ext>."""
    per_hour = {}
    for obj in objects:
        day, hour = obj["Key"].split("/")[2][3:], obj["Key"].split("/")[3][5:]
        stamp = datetime.fromisoformat(f"{day}T{hour}:00:00+00:00")
        n, size = per_hour.get(stamp, (0, 0))
        per_hour[stamp] = (n + (lake.count_from_name(obj["Key"]) or 0), size + obj["Size"])
    return per_hour


def find_holes(events: dict, start: datetime, now: datetime):
    """Finished hours from `start` up to `now`: (those with no events, those under half the typical hour)."""
    hours = []
    hour = start
    while hour < now:
        hours.append(hour)
        hour += timedelta(hours=1)
    typical = median(events.get(h, 0) for h in hours) if hours else 0
    empty = [h for h in hours if not events.get(h)]
    low = [h for h in hours if 0 < events.get(h, 0) < LOW_SHARE * typical]
    return empty, low


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=48)
    args = parser.parse_args()

    s3 = lake.client()
    objects = list_objects(s3, "raw/edits/", ".jsonl.gz")
    if not objects:
        print(f"No files yet in s3://{lake.BUCKET}/raw/edits/. The sink writes its first files after FLUSH_SECONDS.")
        return

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    since = now - timedelta(hours=args.hours - 1)
    per_hour = hourly_counts(objects)
    clean = {hour: n for hour, (n, _) in hourly_counts(list_objects(s3, "clean/edits/", ".parquet")).items()}
    gaps, low = find_holes(clean, max(min(per_hour), WINDOW_START), now)

    first = max(min(per_hour), since)
    print(f"{'hour (UTC)':<17} {'events':>9} {'clean':>9} {'MB':>7}")
    hour = first
    while hour <= now:
        n, size = per_hour.get(hour, (0, 0))
        flag = "   <- GAP" if hour in gaps else "   <- LOW" if hour in low else ""
        print(f"{hour:%Y-%m-%d %H}:00 {n:>9,} {clean.get(hour, 0):>9,} {size / 1e6:>7.1f}{flag}")
        hour += timedelta(hours=1)

    age = datetime.now(timezone.utc) - max(o["LastModified"] for o in objects)
    total_mb = sum(o["Size"] for o in objects) / 1e6
    state = "running" if age < STALE_AFTER else f"STALE (no new file for {age.total_seconds() / 60:.0f} min)"
    last_24h = sum(n for h, (n, _) in per_hour.items() if now - timedelta(hours=24) < h <= now)
    gap_text = ", ".join(f"{g:%d %b %H}:00" for g in gaps) or "none"
    low_text = ", ".join(f"{g:%d %b %H}:00" for g in low) or "none"
    print(f"\ncollector: {state} · last 24 h: {last_24h:,} events · raw zone: {total_mb:,.0f} MB"
          f" · gaps: {gap_text} · low hours: {low_text}")


if __name__ == "__main__":
    main()
