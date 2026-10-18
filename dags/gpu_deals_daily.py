"""Daily GPU deals ingestion: one snapshot per logical date + row-count check + transformation into staging.

Reruns/backfills are safe — loads are idempotent (delete+reload per
snapshot_date). Upstream publication gaps are marked `skipped`, not failed.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from src.ingestion.db import get_conn
from src.ingestion.gpu_deals import UpstreamGapError, ingest_date
from src.transformation.gpu_deals import build as build_staging

MIN_EXPECTED_ROWS = 1000


@dag(
    dag_id="gpu_deals_daily",
    description="Ingest the daily GPU deals snapshot into the raw layer.",
    schedule="0 6 * * *",
    start_date=datetime(2025, 9, 19),
    catchup=False,
    tags=["ingestion", "gpu_deals", "transformation"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def gpu_deals_daily():
    @task
    def load_snapshot() -> str:
        # Scheduled/backfill runs carry a logical date; ad-hoc manual runs
        # (Airflow 3) do not — those ingest "today".
        logical_date = get_current_context().get("logical_date")
        day = logical_date.date() if logical_date else date.today()
        mode = "daily" if day == date.today() else "backfill"
        try:
            ingest_date(day, mode=mode)
        except UpstreamGapError as exc:
            raise AirflowSkipException(str(exc)) from exc
        return day.isoformat()

    @task
    def verify_snapshot(day_iso: str) -> str:
        day = date.fromisoformat(day_iso)
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM raw.gpu_deals WHERE snapshot_date = %s",
                    (day,),
                )
                count = cur.fetchone()[0]
        finally:
            conn.close()
        if count < MIN_EXPECTED_ROWS:
            raise ValueError(f"{day}: only {count} rows loaded (< {MIN_EXPECTED_ROWS})")
        print(f"verified {day}: {count} rows")
        return day_iso

    @task
    def transform_snapshot(day_iso: str) -> None:
       day = date.fromisoformat(day_iso)
       inserted = build_staging(day)
       print(f"transformed {day}: {inserted} rows into staging.gpu_deals")
       if inserted < MIN_EXPECTED_ROWS:
            raise ValueError(f"{day}: transform produced only {inserted} rows (< {MIN_EXPECTED_ROWS})")

    transform_snapshot(verify_snapshot(load_snapshot()))


gpu_deals_daily()
