"""Score the champion model -> mart.gpu_buy_wait (the daily Buy Now / Wait table).

Loads `models:/gpu-buy-wait@champion` from MLflow, builds the same past-only
features as training, scores one snapshot date and writes one row per GPU
type with the advice, the Wait score and today's price.

Grain: one row per (chipset, condition_group, snapshot_date).
Rerunnable: DELETE + INSERT for the scored date, in one transaction.

Usage:
    uv run python -m src.training.predict                    # latest date
    uv run python -m src.training.predict --date 2026-11-18
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from ..ingestion.db import get_conn
from .features import NUMERIC_FEATURES, build_features
from .train import REGISTERED_MODEL

# Download artifacts through the MLflow server: the direct MinIO links it
# would hand out point at objectstorage:9000, which the host can't resolve.
os.environ.setdefault("MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD", "false")

TARGET = "mart.gpu_buy_wait"
ALIAS = "champion"

DDL = f"""
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS {TARGET} (
    chipset          TEXT        NOT NULL,
    condition_group  TEXT        NOT NULL,
    snapshot_date    DATE        NOT NULL,
    advice           TEXT        NOT NULL CHECK (advice IN ('Buy Now', 'Wait')),
    wait_score       NUMERIC(8, 4) NOT NULL,
    rank_in_day      INTEGER     NOT NULL,
    median_price     NUMERIC(10, 2) NOT NULL,
    price_change_7d  NUMERIC(8, 4),
    vram_gb          NUMERIC(6, 2),
    price_per_gb     NUMERIC(10, 2),
    features_complete BOOLEAN    NOT NULL,
    model_name       TEXT        NOT NULL,
    model_version    INTEGER     NOT NULL,
    scored_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chipset, condition_group, snapshot_date)
);
"""

INSERT_SQL = f"""
INSERT INTO {TARGET} (
    chipset, condition_group, snapshot_date, advice, wait_score, rank_in_day,
    median_price, price_change_7d, vram_gb, price_per_gb, features_complete,
    model_name, model_version
) VALUES %s
"""


def load_champion():
    """Return (sklearn estimator, version tags, version number)."""
    import mlflow
    import mlflow.sklearn
    from mlflow import MlflowClient

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001"))
    mv = MlflowClient().get_model_version_by_alias(REGISTERED_MODEL, ALIAS)
    model = mlflow.sklearn.load_model(f"models:/{REGISTERED_MODEL}@{ALIAS}")
    return model, mv.tags, int(mv.version)


def score(model, kind: str, X: pd.DataFrame) -> np.ndarray:
    """Same convention as training: higher = more likely Wait."""
    if kind == "classifier":
        return model.predict_proba(X)[:, 1]
    return -model.predict(X)


def run(day: date | None = None) -> int:
    model, tags, version = load_champion()
    features = tags.get("features", "").split(",")
    if features != NUMERIC_FEATURES:
        raise RuntimeError("champion was trained on a different feature list — retrain first")
    threshold = float(tags["threshold"])

    feats = build_features()
    day = day or feats["snapshot_date"].max().date()
    today = feats[feats["snapshot_date"] == pd.Timestamp(day)].copy()
    if today.empty:
        raise RuntimeError(f"no GPU prices for {day} in staging.gpu_daily_prices")

    today["wait_score"] = score(model, tags["kind"], today[NUMERIC_FEATURES])
    today["advice"] = np.where(today["wait_score"] >= threshold, "Wait", "Buy Now")
    # price history gaps (e.g. missing upstream archive days) leave the return
    # features empty; the advice then rests on fewer signals, so flag it
    today["features_complete"] = today[["ret_7d", "ret_14d", "ret_30d"]].notna().all(axis=1)
    # rank 1 = strongest "Buy Now" signal of the day
    today["rank_in_day"] = today["wait_score"].rank(method="first").astype(int)

    def num(v):
        return None if pd.isna(v) else float(v)

    rows = [
        (r.chipset, r.condition_group, day, r.advice, round(float(r.wait_score), 4),
         int(r.rank_in_day), round(float(r.price_now), 2), num(r.ret_7d),
         num(r.vram_gb), num(r.price_per_gb), bool(r.features_complete), tags.get("model_name", "?"), version)
        for r in today.itertuples()
    ]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(f"DELETE FROM {TARGET} WHERE snapshot_date = %s", (day,))
            execute_values(cur, INSERT_SQL, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    n_wait = int((today["advice"] == "Wait").sum())
    n_incomplete = int((~today["features_complete"]).sum())
    if n_incomplete:
        print(f"WARNING {day}: {n_incomplete}/{len(rows)} GPU types lack 7/14/30-day price "
              "history (gaps in the upstream archive) -> features_complete = false")
    print(f"{day}: scored {len(rows)} GPU types with {REGISTERED_MODEL} v{version} "
          f"({len(rows) - n_wait} Buy Now, {n_wait} Wait) -> {TARGET}")
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--date", type=date.fromisoformat, default=None,
                   help="snapshot date to score (default: latest in staging.gpu_daily_prices)")
    args = p.parse_args()
    run(args.date)


if __name__ == "__main__":
    sys.exit(main())
