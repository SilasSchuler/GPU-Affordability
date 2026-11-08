"""GPU deals ingestion (source: HardwareDealsCo/gpu-deals on GitHub).

Modes:
  seed [--limit N]
      load the already-downloaded local files data/GPU/GPU-*.json
  daily [--date YYYY-MM-DD]
      load one day; today falls back to current-gpu-deals.json, an upstream
      gap is skipped with a warning (exit 0)
  verify [--date YYYY-MM-DD] [--min-rows N]
      fail if raw.gpu_deals holds fewer than N rows for the day
  backfill [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--force]
      fetch missing daily files from the GitHub historical archive;
      --force reloads days that are already present

Every snapshot lands twice:
  1. MinIO  raw/gpu-deals/date=YYYY-MM-DD/GPU-YYYY-MM-DD.json (untouched bytes)
  2. Postgres raw.gpu_deals (one JSONB row per listing)

Idempotent: each snapshot_date is loaded as DELETE + INSERT in a single
transaction, so re-running a day never creates duplicates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import logging
from datetime import date, timedelta

import requests
from psycopg2.extras import Json, execute_values

from ..errors import TooFewRowsError
from .config import GPU_DATA_DIR, GPU_REPO_RAW, MIN_EXPECTED_ROWS
from .db import IngestionRun, ensure_schema, get_conn, set_source_hash
from .storage import ensure_bucket, get_client, upload_bytes

logger = logging.getLogger(__name__)

SOURCE = "gpu_deals"
REQUIRED_KEYS = ("title", "brand", "price", "link", "chipset")
MIN_ROWS, MAX_ROWS = 1_000, 20_000
FILENAME_RE = re.compile(r"GPU-(\d{4}-\d{2}-\d{2})\.json$")
FIRST_KNOWN_DATE = date(2025, 9, 19)  # first snapshot in the upstream archive


def minio_key(day: date) -> str:
    return f"gpu-deals/date={day.isoformat()}/GPU-{day.isoformat()}.json"


def hash_key(day: date) -> str:
    """Key in raw.source_versions for the content hash of one day's snapshot."""
    return f"{SOURCE}:{day.isoformat()}"


def check_not_stale(day: date, raw_bytes: bytes) -> None:
    """Warn if the current-file fallback equals the most recent earlier day.

    Only logs. The upstream source sometimes publishes late, so an identical
    file can still be the latest available. The snapshot loads as normal.
    """
    content_hash = hashlib.sha256(raw_bytes).hexdigest()
    conn = get_conn()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, content_hash FROM raw.source_versions
                 WHERE source LIKE %s AND source < %s
                 ORDER BY source DESC LIMIT 1
                """,
                (f"{SOURCE}:%", hash_key(day)),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if row and row[1] == content_hash:
        logger.warning(
            "current-gpu-deals.json for %s is identical to %s", day, row[0]
        )



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
        set_source_hash(conn, hash_key(day), hashlib.sha256(raw_bytes).hexdigest())
    finally:
        conn.close()


def already_loaded(conn) -> set[date]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT snapshot_date FROM raw.gpu_deals")
        return {r[0] for r in cur.fetchall()}


def count_snapshot_rows(conn, day: date) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.gpu_deals WHERE snapshot_date = %s", (day,))
        return cur.fetchone()[0]


def verify_snapshot(day: date, min_rows: int = MIN_EXPECTED_ROWS) -> int:
    """Fail loudly if the day's snapshot is missing or suspiciously small."""
    conn = get_conn()
    try:
        count = count_snapshot_rows(conn, day)
    finally:
        conn.close()
    if count < min_rows:
        raise TooFewRowsError("verify raw.gpu_deals", count, min_rows, day)
    print(f"verified {day}: {count} rows")
    return count


def load_mode(day: date, today: date) -> str:
    """Run-log mode for a day: only today's load counts as a daily run."""
    return "daily" if day == today else "backfill"


# ------------------------------------------------------------------ fetch

class UpstreamGapError(Exception):
    """The source has no file for this date (upstream publication gap)."""


def fetch_historical(day: date) -> bytes | None:
    """The archived daily file for `day`, or None if upstream has no gap."""
    url = f"{GPU_REPO_RAW}/historical-gpu-deals-data/GPU-{day.isoformat()}.json"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.content


def fetch_current() -> bytes:
    resp = requests.get(f"{GPU_REPO_RAW}/current-gpu-deals.json", timeout=60)
    resp.raise_for_status()
    return resp.content


def ingest_date(day: date, mode: str, today: date | None = None) -> None:
    """Load the snapshot for one logical date (used by CLI and Airflow DAG).

    Historical file first; only for today fall back to current-gpu-deals.json
    (the archive file for today may not be published yet). Any other 404 is
    an upstream gap and raises UpstreamGapError.
    """
    today = today or date.today()  # injectable so tests and the CLI share one clock
    raw = fetch_historical(day)
    source_file = f"GPU-{day.isoformat()}.json"
    if raw is None:
        if day == today:
            raw = fetch_current()
            check_not_stale(day, raw)  # before MinIO/Postgres see it
            source_file = "current-gpu-deals.json"
        else:
            raise UpstreamGapError(f"no upstream file for {day}")
    load_snapshot(day, raw, source_file=source_file, mode=mode)


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


def mode_daily(day: date, today: date) -> None:
    try:
        ingest_date(day, load_mode(day, today), today)
    except UpstreamGapError as exc:
        # A gap is upstream's fault, not a pipeline failure; verify catches real emptiness.
        logger.warning("skipped %s: %s", day, exc)
        print(f"daily: {day} skipped (upstream gap)")
        return
    print(f"daily: {day} ok")


def mode_backfill(start: date, end: date, force: bool = False) -> None:
    loaded: set[date] = set()
    if not force:
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
        try:
            ingest_date(day, mode="backfill")
            print(f"  {day} ok")
        except UpstreamGapError:
            print(f"  {day} upstream gap (404), skipped")
        day += timedelta(days=1)


# ------------------------------------------------------------------ cli

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="mode", required=True)

    sp = sub.add_parser("seed", help="load local data/GPU/*.json files")
    sp.add_argument("--limit", type=int, default=None, help="only first N files")

    sp = sub.add_parser("daily", help="load one day (default: today)")
    sp.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="logical snapshot date YYYY-MM-DD (default: today)",
    )

    sp = sub.add_parser("verify", help="check the row count of one day's snapshot")
    sp.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="snapshot date YYYY-MM-DD (default: today)",
    )
    sp.add_argument(
        "--min-rows",
        type=int,
        default=MIN_EXPECTED_ROWS,
        help=f"minimum rows required (default: {MIN_EXPECTED_ROWS})",
    )

    sp = sub.add_parser("backfill", help="fetch missing dates from GitHub archive")
    sp.add_argument(
        "--start",
        type=date.fromisoformat,
        default=None,
        help=f"first date YYYY-MM-DD (default: {FIRST_KNOWN_DATE})",
    )
    sp.add_argument(
        "--end",
        type=date.fromisoformat,
        default=None,
        help="last date YYYY-MM-DD (default: today)",
    )
    sp.add_argument(
        "--force",
        action="store_true",
        help="reload dates that are already in raw.gpu_deals",
    )

    args = p.parse_args()
    today = date.today()
    if args.mode == "seed":
        mode_seed(args.limit)
    elif args.mode == "daily":
        mode_daily(args.date or today, today)
    elif args.mode == "verify":
        verify_snapshot(args.date or today, args.min_rows)
    elif args.mode == "backfill":
        start = args.start or FIRST_KNOWN_DATE
        end = args.end or today
        if start > end:
            p.error(f"--start {start} is after --end {end}")
        mode_backfill(start, end, args.force)


if __name__ == "__main__":
    sys.exit(main())