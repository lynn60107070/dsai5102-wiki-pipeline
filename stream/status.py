"""Collector status: events per hour in the raw zone, gaps, and whether files are still arriving.

Run: `docker compose exec raw-sink python -m stream.status` (add `--hours 72` to look further back).
The last line is the one-line status for the daily update.
"""
import argparse
import gzip
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
STALE_AFTER_S = 2 * int(os.environ.get("FLUSH_SECONDS", "300")) + 60


def count_lines(path: Path) -> int:
    with gzip.open(path, "rb") as f:
        return sum(1 for _ in f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=48)
    args = parser.parse_args()

    root = DATA_DIR / "raw" / "edits"
    files = sorted(root.glob("dt=*/hour=*/*.jsonl.gz"))
    if not files:
        print(f"No files yet in {root}. The sink writes its first files after FLUSH_SECONDS.")
        return

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    since = now - timedelta(hours=args.hours - 1)
    per_hour = {}
    for path in files:
        day, hour = path.parent.parent.name[3:], path.parent.name[5:]
        stamp = datetime.fromisoformat(f"{day}T{hour}:00:00+00:00")
        if stamp >= since:
            n, size = per_hour.get(stamp, (0, 0))
            per_hour[stamp] = (n + count_lines(path), size + path.stat().st_size)

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

    age = time.time() - max(p.stat().st_mtime for p in files)
    total_mb = sum(p.stat().st_size for p in files) / 1e6
    state = "running" if age < STALE_AFTER_S else f"STALE (no new file for {age / 60:.0f} min)"
    last_24h = sum(n for h, (n, _) in per_hour.items() if h > now - timedelta(hours=24))
    gap_text = ", ".join(f"{g:%d %b %H}:00" for g in gaps) or "none"
    print(f"\ncollector: {state} · last 24 h: {last_24h:,} events · raw zone: {total_mb:,.0f} MB · gaps: {gap_text}")


if __name__ == "__main__":
    main()
