"""Weekly Epoch AI models refresh + release calendar rebuild.

The CSV is semi-static: download it, load only when the sha256 changed
(logged as `unchanged` otherwise), then rebuild staging.model_releases
(cheap, idempotent — runs every time). Manual triggering works too.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.sdk import dag, get_current_context, task

from src.ingestion.epoch_models import mode_fetch
from src.transformation.model_releases import build


@dag(
    dag_id="epoch_models_weekly",
    description="Refresh the Epoch AI models CSV when changed; rebuild the release calendar.",
    schedule="0 7 * * 1",
    start_date=datetime(2026, 1, 5),
    catchup=False,
    tags=["ingestion", "epoch_models", "transformation"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def epoch_models_weekly():
    @task
    def fetch_and_load() -> None:
        # Manual runs (Airflow 3) have no logical date — use today then.
        logical_date = get_current_context().get("logical_date")
        day = logical_date.date() if logical_date else date.today()
        mode_fetch(day)

    @task
    def build_model_releases() -> None:
        count = build()
        print(f"staging.model_releases rebuilt: {count} rows")

    fetch_and_load() >> build_model_releases()


epoch_models_weekly()
