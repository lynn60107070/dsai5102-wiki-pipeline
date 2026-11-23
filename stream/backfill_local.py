"""One-off: copy the raw files collected on the local volume before MinIO existed into the lake.

Keeps the same dt=/hour= layout, adds the event count to each object name, verifies every upload by
reading its size back, and never deletes the local files (remove the volume yourself once the counts match).

Run: `docker compose exec raw-sink python -m stream.backfill_local`
"""
import gzip
import os
from pathlib import Path

from stream import lake

LOCAL_ROOT = Path(os.environ.get("DATA_DIR", "/data"))


def main():
    s3 = lake.client()
    files = sorted((LOCAL_ROOT / "raw" / "edits").glob("dt=*/hour=*/*.jsonl.gz"))
    total = 0
    for path in files:
        data = path.read_bytes()
        count = sum(1 for _ in gzip.decompress(data).splitlines())
        prefix = path.parent.relative_to(LOCAL_ROOT).as_posix()
        key = f"{prefix}/{path.name.removesuffix('.jsonl.gz')}-n{count}.jsonl.gz"
        s3.put_object(Bucket=lake.BUCKET, Key=key, Body=data, ContentType="application/gzip")
        if s3.head_object(Bucket=lake.BUCKET, Key=key)["ContentLength"] != len(data):
            raise SystemExit(f"Size mismatch after upload: {key}")
        total += count
        print(f"{key}  {count:,} events")
    print(f"\nCopied {len(files)} files, {total:,} events, into s3://{lake.BUCKET}/raw/edits/")


if __name__ == "__main__":
    main()
