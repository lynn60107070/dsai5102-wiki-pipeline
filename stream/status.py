"""Collector status: events per hour in the raw zone (MinIO), gaps, and whether files are still arriving.

Run: `docker compose exec raw-sink python -m stream.status` (add `--hours 72` to look further back).
The last line is the one-line status for the daily update. Counts come from the object names, so
nothing is downloaded.
"""
import argparse
import os
from datetime import datetime, timedelta, timezone

from stream import lake

STALE_AFTER = timedelta(seconds=2 * int(os.environ.get("FLUSH_SECONDS", "300")) + 60)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=48)
    args = parser.parse_args()

    s3 = lake.client()
    objects = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=lake.BUCKET, Prefix="raw/edits/"):
        objects.extend(page.get("Contents", []))
    objects = [o for o in objects if o["Key"].endswith(".jsonl.gz")]
    if not objects:
        print(f"No files yet in s3://{lake.BUCKET}/raw/edits/. The sink writes its first files after FLUSH_SECONDS.")
        return

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    since = now - timedelta(hours=args.hours - 1)
    per_hour = {}
    for obj in objects:
        day, hour = obj["Key"].split("/")[2][3:], obj["Key"].split("/")[3][5:]
        stamp = datetime.fromisoformat(f"{day}T{hour}:00:00+00:00")
        if stamp >= since:
            n, size = per_hour.get(stamp, (0, 0))
            per_hour[stamp] = (n + (lake.count_from_name(obj["Key"]) or 0), size + obj["Size"])

    first = max(min(per_hour, default=now), since)
    print(f"{'hour (UTC)':<17} {'events':>9} {'MB':>7}")
    gaps = []
    hour = first
    while hour <= now:
        n, size = per_hour.get(hour, (0, 0))
        if n == 0 and hour < now:
            gaps.append(hour)
        print(f"{hour:%Y-%m-%d %H}:00 {n:>9,} {size / 1e6:>7.1f}{'   <- GAP' if n == 0 and hour < now else ''}")
        hour += timedelta(hours=1)

    age = datetime.now(timezone.utc) - max(o["LastModified"] for o in objects)
    total_mb = sum(o["Size"] for o in objects) / 1e6
    state = "running" if age < STALE_AFTER else f"STALE (no new file for {age.total_seconds() / 60:.0f} min)"
    last_24h = sum(n for h, (n, _) in per_hour.items() if h > now - timedelta(hours=24))
    gap_text = ", ".join(f"{g:%d %b %H}:00" for g in gaps) or "none"
    print(f"\ncollector: {state} · last 24 h: {last_24h:,} events · raw zone: {total_mb:,.0f} MB · gaps: {gap_text}")


if __name__ == "__main__":
    main()
