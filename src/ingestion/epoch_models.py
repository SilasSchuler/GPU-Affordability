"""Epoch AI Notable Models ingestion (semi-static CSV source).

Modes:
  fetch    — download epoch.ai/data/notable_ai_models.csv (CC-BY), load only
             when the sha256 changed (tracked in raw.source_versions)
  snapshot — load a local CSV file (offline fallback / exact reproduction)

Lands the raw file in MinIO (raw/epoch-ai/load_date=...) and one JSONB row
per record in raw.epoch_models. Idempotent per load_date; new days = new
versions. No filtering here — the >= 2020 business rule lives in
src/transformation/model_releases.py.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from datetime import date
from pathlib import Path

import requests
from psycopg2.extras import Json, execute_values

from .config import EPOCH_CSV, EPOCH_CSV_URL
from .db import (
    IngestionRun,
    ensure_schema,
    get_conn,
    get_source_hash,
    set_source_hash,
)
from .storage import ensure_bucket, get_client, upload_bytes

SOURCE = "epoch_models"
CSV_NAME = "notable_ai_models.csv"
MIN_ROWS = 500
EXPECTED_COLUMNS = {"Model", "Organization", "Publication date"}


def minio_key(day: date) -> str:
    return f"epoch-ai/load_date={day.isoformat()}/{CSV_NAME}"


def validate(records: list[dict], fieldnames: list[str]) -> None:
    """Hard checks. Raises before anything is written to MinIO or Postgres."""
    problems = []
    if len(records) < MIN_ROWS:
        problems.append(f"too few rows: {len(records)} (< {MIN_ROWS})")
    missing = EXPECTED_COLUMNS - set(fieldnames or [])
    if missing:
        problems.append(f"expected columns missing: {sorted(missing)}")
    if problems:
        raise ValueError("epoch csv rejected: " + "; ".join(problems))


def load_csv(raw_bytes: bytes, filename: str, day: date, mode: str) -> int:
    """Validate, then land one CSV version in MinIO + Postgres. Returns row count.

    Everything runs inside IngestionRun, so a rejected file is logged as
    'failed' in raw.ingestion_log. The caller saves the sha256 only after this
    returns, so a rejected file is retried on the next run.
    """
    conn = get_conn()
    try:
        ensure_schema(conn)
        with IngestionRun(conn, SOURCE, mode, snapshot_date=day) as run:
            text = raw_bytes.decode("utf-8-sig")  # tolerate BOM
            reader = csv.DictReader(text.splitlines())
            records = list(reader)
            validate(records, reader.fieldnames or [])

            s3 = get_client()
            ensure_bucket(s3)
            upload_bytes(s3, minio_key(day), raw_bytes)

            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM raw.epoch_models WHERE load_date = %s", (day,)
                )
                execute_values(
                    cur,
                    "INSERT INTO raw.epoch_models (load_date, source, payload) VALUES %s",
                    [(day, filename, Json(r)) for r in records],
                    page_size=1000,
                )
            conn.commit()
            run.rows_loaded = len(records)
            run.message = "checks ok"
    finally:
        conn.close()
    return len(records)


def hash_of(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()


def mode_fetch(day: date) -> None:
    """Download the current upstream CSV; load only if changed."""
    resp = requests.get(EPOCH_CSV_URL, timeout=120)
    resp.raise_for_status()
    raw_bytes = resp.content
    content_hash = hash_of(raw_bytes)

    conn = get_conn()
    try:
        ensure_schema(conn)
        previous = get_source_hash(conn, SOURCE)
        if previous == content_hash:
            with IngestionRun(conn, SOURCE, "fetch", snapshot_date=day) as run:
                run.rows_loaded = 0
                run.message = "unchanged (sha256 match), nothing loaded"
            print(f"fetch: {day} unchanged (sha256 {content_hash[:12]}…)")
            return
    finally:
        conn.close()

    rows = load_csv(raw_bytes, CSV_NAME, day, mode="fetch")
    conn = get_conn()
    try:
        set_source_hash(conn, SOURCE, content_hash)
    finally:
        conn.close()
    print(f"fetch: {day} changed -> loaded {rows} rows ({content_hash[:12]}…)")


def mode_snapshot(csv_path: Path, day: date) -> None:
    rows = load_csv(csv_path.read_bytes(), csv_path.name, day, mode="snapshot")
    print(f"snapshot: {day} ok ({rows} rows from {csv_path.name})")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="mode", required=True)

    sp = sub.add_parser("fetch", help="download the upstream CSV (hash-checked)")
    sp.add_argument("--date", type=date.fromisoformat, default=None)

    sp = sub.add_parser("snapshot", help="load a local CSV file")
    sp.add_argument("--file", type=str, default=str(EPOCH_CSV))
    sp.add_argument("--date", type=date.fromisoformat, default=None)

    args = p.parse_args()
    today = date.today()
    if args.mode == "fetch":
        mode_fetch(args.date or today)
    elif args.mode == "snapshot":
        mode_snapshot(Path(args.file), args.date or today)


if __name__ == "__main__":
    sys.exit(main())