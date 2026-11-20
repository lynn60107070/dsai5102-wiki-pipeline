"""Copy a few compacted hours of the clean zone into sample/ (committed to git) for sample-data mode.

The sample lets someone who clones the repo see a working dashboard straight away, without waiting
for the stream to collect hours of data. It contains no user names (the clean zone never has them).
Pick finished, compacted hours (run compaction first if needed). About 3 MB per hour.

It also copies the pageview files for the same hours plus the 24 hours before them, so the attention
tile has the baseline it needs to flag spikes in the sample hours (about 2 MB per hour).

Run on your laptop, with the stack up:
  python scripts/make_sample.py 2026-10-25 6 11       # date, first hour, last hour (UTC)
"""
import argparse
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "sample"  # short paths: Windows limits full paths to 260 characters


def load_env() -> dict:
    values = {}
    for name in (".env.example", ".env"):
        path = ROOT / name
        if path.exists():
            for line in path.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    values[key.strip()] = value.strip()
    values.update({k: v for k, v in os.environ.items() if k in values})
    return values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("day", help="UTC date, e.g. 2026-10-25")
    parser.add_argument("first_hour", type=int)
    parser.add_argument("last_hour", type=int)
    args = parser.parse_args()

    env = load_env()
    s3 = boto3.client("s3", endpoint_url="http://localhost:9000", region_name="us-east-1",
                      aws_access_key_id=env["MINIO_ROOT_USER"], aws_secret_access_key=env["MINIO_ROOT_PASSWORD"],
                      config=Config(s3={"addressing_style": "path"}))
    shutil.rmtree(SAMPLE, ignore_errors=True)
    total = 0
    for hour in range(args.first_hour, args.last_hour + 1):
        prefix = f"clean/edits/dt={args.day}/hour={hour:02d}/"
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket="lake", Prefix=prefix).get("Contents", [])]
        if len(keys) != 1 or "compacted" not in keys[0]:
            raise SystemExit(f"{prefix} has {len(keys)} files; run the compaction first (warehouse_hourly does)")
        count = keys[0].rsplit("-n", 1)[1]  # "<events>.parquet"
        target = SAMPLE / prefix / f"sample-n{count}"
        target.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file("lake", keys[0], str(target))
        total += target.stat().st_size
        print(f"{keys[0]}  {target.stat().st_size / 1e6:.1f} MB")

    # Pageviews: a spike needs the previous 24 hours as its baseline (mart_attention_vs_edits).
    hour = datetime.fromisoformat(args.day) + timedelta(hours=args.first_hour - 24)
    last = datetime.fromisoformat(args.day) + timedelta(hours=args.last_hour)
    while hour <= last:
        prefix = f"raw/pageviews/dt={hour:%Y-%m-%d}/hour={hour:%H}/"
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket="lake", Prefix=prefix).get("Contents", [])]
        if not keys:
            raise SystemExit(f"{prefix} is empty; load that hour with pageviews_hourly first")
        for key in keys:
            target = SAMPLE / key
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file("lake", key, str(target))
            total += target.stat().st_size
        hour += timedelta(hours=1)
    print(f"sample: {total / 1e6:.1f} MB in {SAMPLE.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
