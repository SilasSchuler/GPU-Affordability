"""GPU deals ingestion (source: HardwareDealsCo/gpu-deals on GitHub).

Modes:
  seed     — load the already-downloaded local files data/GPU/GPU-*.json
  backfill — fetch missing daily files from the GitHub historical archive
  daily    — fetch current-gpu-deals.json and stamp it with the run date

Every snapshot lands twice:
  1. MinIO  raw/gpu-deals/date=YYYY-MM-DD/GPU-YYYY-MM-DD.json (untouched bytes)
  2. Postgres raw.gpu_deals (one JSONB row per listing)

Idempotent: each snapshot_date is loaded as DELETE + INSERT in a single
transaction, so re-running a day never creates duplicates.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, timedelta

import requests
from psycopg2.extras import Json, execute_values

from .config import GPU_DATA_DIR, GPU_REPO_RAW
from .db import IngestionRun, ensure_schema, get_conn
from .storage import ensure_bucket, get_client, upload_bytes

SOURCE = "gpu_deals"
REQUIRED_KEYS = ("title", "brand", "price", "link", "chipset")
MIN_ROWS, MAX_ROWS = 1_000, 20_000
FILENAME_RE = re.compile(r"GPU-(\d{4}-\d{2}-\d{2})\.json$")
FIRST_KNOWN_DATE = date(2025, 9, 19)  # first snapshot in the upstream archive


def minio_key(day: date) -> str:
    return f"gpu-deals/date={day.isoformat()}/GPU-{day.isoformat()}.json"


def run_checks(records: list[dict]) -> str:
    """Lightweight ingest checks — logged, never fixed (transform is W14)."""
    notes = []
    if not (MIN_ROWS <= len(records) <= MAX_ROWS):
        notes.append(f"row count {len(records)} outside [{MIN_ROWS},{MAX_ROWS}]")
    if records:
        missing = {
            k: sum(1 for r in records if r.get(k) in (None, "")) for k in REQUIRED_KEYS
        }
        bad = {k: n for k, n in missing.items() if n}
        if bad:
            notes.append(f"missing/empty required keys: {bad}")
    return "; ".join(notes) if notes else "checks ok"


def load_snapshot(day: date, raw_bytes: bytes, source_file: str, mode: str) -> None:
    """Land one daily snapshot in MinIO + Postgres. Raises on hard errors."""
    s3 = get_client()
    ensure_bucket(s3)
    upload_bytes(s3, minio_key(day), raw_bytes)

    records = json.loads(raw_bytes)  # hard fail if unparseable
    if not isinstance(records, list):
        raise ValueError(f"expected JSON array, got {type(records).__name__}")
    if not records:
        raise ValueError("snapshot contains zero rows")

    conn = get_conn()
    try:
        ensure_schema(conn)
        with IngestionRun(conn, SOURCE, mode, snapshot_date=day) as run:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM raw.gpu_deals WHERE snapshot_date = %s", (day,)
                )
                execute_values(
                    cur,
                    "INSERT INTO raw.gpu_deals (snapshot_date, source_file, payload) VALUES %s",
                    [(day, source_file, Json(r)) for r in records],
                    page_size=1000,
                )
            conn.commit()
            run.rows_loaded = len(records)
            run.message = run_checks(records)
    finally:
        conn.close()


def already_loaded(conn) -> set[date]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT snapshot_date FROM raw.gpu_deals")
        return {r[0] for r in cur.fetchall()}


# ---------------------------------------------------------------- modes

def mode_seed(limit: int | None) -> None:
    files = sorted(GPU_DATA_DIR.glob("GPU-*.json"))
    if limit:
        files = files[:limit]
    print(f"seed: {len(files)} local files")
    for path in files:
        m = FILENAME_RE.search(path.name)
        if not m:
            print(f"  skip {path.name}: unexpected name")
            continue
        day = date.fromisoformat(m.group(1))
        load_snapshot(day, path.read_bytes(), source_file=path.name, mode="seed")
        print(f"  {day} ok")


def mode_daily(day: date) -> None:
    url = f"{GPU_REPO_RAW}/current-gpu-deals.json"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    load_snapshot(day, resp.content, source_file="current-gpu-deals.json", mode="daily")
    print(f"daily: {day} ok ({len(resp.content)} bytes)")


def mode_backfill(start: date, end: date) -> None:
    conn = get_conn()
    try:
        ensure_schema(conn)
        loaded = already_loaded(conn)
    finally:
        conn.close()

    day = start
    while day <= end:
        if day in loaded:
            day += timedelta(days=1)
            continue
        name = f"GPU-{day.isoformat()}.json"
        url = f"{GPU_REPO_RAW}/historical-gpu-deals-data/{name}"
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            print(f"  {day} upstream gap (404), skipped")
            day += timedelta(days=1)
            continue
        resp.raise_for_status()
        load_snapshot(day, resp.content, source_file=name, mode="backfill")
        print(f"  {day} ok")
        day += timedelta(days=1)


# ------------------------------------------------------------------ cli

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="mode", required=True)

    sp = sub.add_parser("seed", help="load local data/GPU/*.json files")
    sp.add_argument("--limit", type=int, default=None, help="only first N files")

    sub.add_parser("backfill", help="fetch missing dates from GitHub archive")

    sp = sub.add_parser("daily", help="fetch current-gpu-deals.json")
    sp.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today(),
        help="logical snapshot date (default: today)",
    )

    args = p.parse_args()
    if args.mode == "seed":
        mode_seed(args.limit)
    elif args.mode == "backfill":
        mode_backfill(FIRST_KNOWN_DATE, date.today())
    elif args.mode == "daily":
        mode_daily(args.date)


if __name__ == "__main__":
    sys.exit(main())
