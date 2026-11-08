"""Weekly full rebuild of staging.gpu_deals.

Why: the daily DAG only builds its own day, so deals already in staging keep
their old release link when Epoch adds a back-dated model. build_history()
re-links every deal against the current release calendar. This run also gurantees
that late gpu releases are corrected with the daily files.

Safe to rerun: TRUNCATE + INSERT inside one transaction, holding the staging
advisory lock. A concurrent daily run waits for the lock instead of
overlapping. Raw stays intact.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.transformation.gpu_deals import build_history

MIN_EXPECTED_ROWS = 1000


@dag(
    dag_id="gpu_deals_relink_weekly",
    description="Rebuild staging.gpu_deals from raw so all deals match the current release calendar.",
    schedule="0 8 * * 0",  # Sunday 08:00, after the 05:00 Epoch and 06:00 GPU DAGs
    start_date=datetime(2026, 1, 4),
    catchup=False,
    max_active_runs=1,
    tags=["transformation", "gpu_deals"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def gpu_deals_relink_weekly():
    @task(execution_timeout=timedelta(hours=1))
    def rebuild_history() -> None:
        count = build_history()
        print(f"staging.gpu_deals rebuilt: {count} rows")
        if count < MIN_EXPECTED_ROWS:
            raise ValueError(f"rebuild produced only {count} rows (< {MIN_EXPECTED_ROWS})")

    rebuild_history()


gpu_deals_relink_weekly()