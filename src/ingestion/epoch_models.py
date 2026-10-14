"""Epoch AI Notable Models ingestion (semi-static CSV snapshot).

Mode:
  snapshot — load the local CSV (data/epoch_ai_models_release/) as one
             versioned snapshot.

Lands twice:
  1. MinIO  raw/epoch-ai/load_date=YYYY-MM-DD/notable_models.csv (untouched bytes)
  2. Postgres raw.epoch_models (one JSONB row per CSV record)

Idempotent per load_date: re-running on the same day deletes and reloads
that day's rows. Loading on a different day creates a new version — the
source is semi-static, so new versions are meaningful.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date

from psycopg2.extras import Json, execute_values

from .config import EPOCH_CSV
from .db import IngestionRun, ensure_schema, get_conn
from .storage import ensure_bucket, get_client, upload_bytes

SOURCE = "epoch_models"


def minio_key(day: date) -> str:
    return f"epoch-ai/load_date={day.isoformat()}/notable_models.csv"


def run_checks(records: list[dict], fieldnames: list[str]) -> str:
    notes = []
    if len(records) < 500:
        notes.append(f"suspiciously few rows: {len(records)}")
    expected = {"Model", "Organization", "Publication date"}
    missing = expected - set(fieldnames or [])
    if missing:
        notes.append(f"expected columns missing: {sorted(missing)}")
    return "; ".join(notes) if notes else "checks ok"


def mode_snapshot(csv_path, day: date) -> None:
    raw_bytes = csv_path.read_bytes()

    s3 = get_client()
    ensure_bucket(s3)
    upload_bytes(s3, minio_key(day), raw_bytes)

    text = raw_bytes.decode("utf-8-sig")  # tolerate BOM
    reader = csv.DictReader(text.splitlines())
    records = list(reader)
    if not records:
        raise ValueError("CSV contains zero data rows")

    conn = get_conn()
    try:
        ensure_schema(conn)
        with IngestionRun(conn, SOURCE, "snapshot", snapshot_date=day) as run:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM raw.epoch_models WHERE load_date = %s AND source = %s",
                    (day, csv_path.name),
                )
                execute_values(
                    cur,
                    "INSERT INTO raw.epoch_models (load_date, source, payload) VALUES %s",
                    [(day, csv_path.name, Json(r)) for r in records],
                    page_size=1000,
                )
            conn.commit()
            run.rows_loaded = len(records)
            run.message = run_checks(records, reader.fieldnames or [])
    finally:
        conn.close()
    print(f"snapshot: {day} ok ({len(records)} rows from {csv_path.name})")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="mode", required=True)
    sp = sub.add_parser("snapshot", help="load the local Epoch CSV")
    sp.add_argument("--file", type=str, default=str(EPOCH_CSV))
    sp.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today(),
        help="logical load date (default: today)",
    )
    args = p.parse_args()

    if args.mode == "snapshot":
        from pathlib import Path

        mode_snapshot(Path(args.file), args.date)


if __name__ == "__main__":
    sys.exit(main())
