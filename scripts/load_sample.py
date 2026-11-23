"""Sample-data mode: upload sample/ into the lake, but ONLY if the lake has no edits for the sample's hours.

On a fresh clone those hours (4 Nov 2026, 06:00-11:00 UTC) are empty, so the bundled sample goes in and the
first warehouse build has data to show. The check looks at the sample's own hours, not the whole clean zone:
on a fresh clone the stream may already have written its first live hour before bootstrap runs, and that
must not stop the sample. Where those hours already hold collected data, this does nothing, so it can
never mix sample rows into real data. Re-uploading writes the same keys, so a second run is harmless.

Run (inside Airflow, by scripts/bootstrap.sh): /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/load_sample.py
"""
import os
from pathlib import Path

import boto3
from botocore.config import Config

SAMPLE = Path(os.environ.get("SAMPLE_DIR", "/opt/airflow/project/sample"))
BUCKET = os.environ.get("LAKE_BUCKET", "lake")


def main():
    s3 = boto3.client("s3", endpoint_url=f"http://{os.environ.get('S3_ENDPOINT_HOST', 'minio:9000')}",
                      aws_access_key_id=os.environ["MINIO_ROOT_USER"],
                      aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
                      region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
    files = sorted(p for p in SAMPLE.rglob("*.parquet"))
    sample_hours = sorted({p.parent.relative_to(SAMPLE).as_posix() + "/"
                           for p in files if p.relative_to(SAMPLE).parts[:2] == ("clean", "edits")})
    for prefix in sample_hours:
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])]
        if any(not k.rsplit("/", 1)[-1].startswith("sample-") for k in keys):
            print(f"sample data: skipped (the lake already has collected edits for {prefix})")
            return
    for path in files:
        s3.upload_file(str(path), BUCKET, path.relative_to(SAMPLE).as_posix())
    print(f"sample data: loaded {len(files)} file(s) into s3://{BUCKET}/ (fresh install)")


if __name__ == "__main__":
    main()
