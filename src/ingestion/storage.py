"""MinIO (S3-compatible) helpers: client, bucket bootstrap, uploads."""

from __future__ import annotations

import boto3
from botocore.client import BaseClient

from .config import RAW_BUCKET, SETTINGS


def get_client() -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.minio_endpoint,
        aws_access_key_id=SETTINGS.minio_admin,
        aws_secret_access_key=SETTINGS.minio_secret,
    )


def ensure_bucket(client: BaseClient, bucket: str = RAW_BUCKET) -> None:
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def upload_bytes(client: BaseClient, key: str, data: bytes, bucket: str = RAW_BUCKET) -> None:
    """Upload raw bytes. Overwriting an identical snapshot is intentional
    (keeps re-runs idempotent)."""
    client.put_object(Bucket=bucket, Key=key, Body=data)
