"""Sample-data mode: upload sample/ into the lake, but ONLY if the clean zone is still empty.

On a fresh clone the lake is empty, so the bundled sample hours go in and the first warehouse build has
data to show. Anywhere the stream has already written data, this does nothing, so it can never mix
sample rows into real data.

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
    if s3.list_objects_v2(Bucket=BUCKET, Prefix="clean/edits/", MaxKeys=1).get("KeyCount", 0):
        print("sample data: skipped (the lake already has data)")
        return
    files = sorted(p for p in SAMPLE.rglob("*.parquet"))
    for path in files:
        s3.upload_file(str(path), BUCKET, path.relative_to(SAMPLE).as_posix())
    print(f"sample data: loaded {len(files)} file(s) into s3://{BUCKET}/ (empty lake, fresh install)")


if __name__ == "__main__":
    main()
