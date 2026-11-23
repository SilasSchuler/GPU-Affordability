"""Feature + label building for the Buy/Wait model.

Input: staging.gpu_daily_prices (one row per chipset, condition, day) and
staging.model_releases. Output: a pandas frame with one row per
(chipset, condition_group, snapshot_date), past-only features and the label.

Label ("Wait" = 1): the typical price ~14 days later (median of the daily
medians on days t+12..t+16) is at least DROP_THRESHOLD below today's price
(median of days t-2..t). Smoothing both ends keeps single noisy eBay days
from flipping the label.

All features use only data on or before the snapshot date, so the same
code serves training and daily scoring without leakage.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from ..ingestion.db import get_conn

HORIZON_DAYS = 14
DROP_THRESHOLD = 0.05
MIN_OBSERVED_DAYS = 45  # series with fewer observed days are too sparse to learn from

NUMERIC_FEATURES = [
    "log_price",
    "ret_3d",
    "ret_7d",
    "ret_14d",
    "ret_30d",
    "range_pos_30d",
    "vol_14d",
    "p25_ratio",
    "min_ratio",
    "log_listings",
    "listings_change_7d",
    "vram_gb",
    "price_per_gb",
    "days_since_llm_release",
    "releases_30d",
    "releases_90d",
    "market_ret_7d",
    "market_ret_14d",
    "rel_to_market_7d",
    "is_new",
    "is_nvidia",
]
LABEL = "wait"
TARGET_RETURN = "future_return"
KEYS = ["chipset", "condition_group", "snapshot_date"]


def _query(sql: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c.name for c in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=cols)


def load_daily_prices() -> pd.DataFrame:
    df = _query(
        """
        SELECT chipset, condition_group, snapshot_date, brand, n_listings,
               median_price, p25_price, min_price, vram_gb,
               median_price_per_gb, days_since_llm_release
        FROM staging.gpu_daily_prices
        """
    )
    if df.empty:
        raise RuntimeError("staging.gpu_daily_prices is empty — run the transformations first")
    num = ["median_price", "p25_price", "min_price", "vram_gb",
           "median_price_per_gb", "days_since_llm_release", "n_listings"]
    df[num] = df[num].astype(float)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    return df


def load_release_dates() -> pd.Series:
    df = _query("SELECT release_date FROM staging.model_releases")
    return pd.to_datetime(df["release_date"]).sort_values().reset_index(drop=True)


def _series_features(g: pd.DataFrame) -> pd.DataFrame:
    """Features + label for one (chipset, condition_group) series."""
    g = g.set_index("snapshot_date").sort_index()
    full = pd.date_range(g.index.min(), g.index.max(), freq="D")
    g = g.reindex(full)  # calendar index: shifts below are in days, gaps stay NaN
    s = g["median_price"]

    price_now = s.rolling(3, min_periods=1).median()
    out = pd.DataFrame(index=full)
    out["observed"] = s.notna()
    out["price_now"] = price_now
    out["log_price"] = np.log(price_now)
    for d in (3, 7, 14, 30):
        out[f"ret_{d}d"] = price_now / price_now.shift(d) - 1
    lo = s.rolling(30, min_periods=5).min()
    hi = s.rolling(30, min_periods=5).max()
    out["range_pos_30d"] = (price_now - lo) / (hi - lo).replace(0, np.nan)
    out["vol_14d"] = s.pct_change(fill_method=None).rolling(14, min_periods=5).std()
    out["p25_ratio"] = g["p25_price"] / s
    out["min_ratio"] = g["min_price"] / s
    listings = g["n_listings"].rolling(3, min_periods=1).mean()
    out["log_listings"] = np.log1p(listings)
    out["listings_change_7d"] = listings / listings.shift(7) - 1
    out["vram_gb"] = g["vram_gb"].ffill()
    out["price_per_gb"] = g["median_price_per_gb"]
    out["days_since_llm_release"] = g["days_since_llm_release"]

    # label: smoothed price around t+HORIZON vs smoothed price today
    future = s.shift(-(HORIZON_DAYS + 2)).rolling(5, min_periods=2).median()
    out[TARGET_RETURN] = future / price_now - 1
    out.index.name = "snapshot_date"
    return out.reset_index()


def build_features(df: pd.DataFrame | None = None,
                   releases: pd.Series | None = None) -> pd.DataFrame:
    """Return one row per observed (chipset, condition_group, day).

    Rows whose label is not yet known (last ~16 days) keep NaN in LABEL /
    TARGET_RETURN — they are the rows to score, not to train on.
    """
    df = load_daily_prices() if df is None else df
    releases = load_release_dates() if releases is None else releases

    observed_days = df.groupby(["chipset", "condition_group"])["snapshot_date"].transform("count")
    df = df[observed_days >= MIN_OBSERVED_DAYS]

    parts = []
    for (chipset, cond), g in df.groupby(["chipset", "condition_group"], sort=False):
        f = _series_features(g)
        f["chipset"] = chipset
        f["condition_group"] = cond
        f["is_new"] = float(cond == "new")
        brand = g["brand"].mode()
        f["is_nvidia"] = float(not brand.empty and brand.iloc[0] == "NVIDIA")
        parts.append(f)
    feats = pd.concat(parts, ignore_index=True)
    feats = feats[feats.pop("observed")].copy()

    # market-wide movement per condition group (are all GPUs moving?)
    for d in (7, 14):
        feats[f"market_ret_{d}d"] = feats.groupby(
            ["condition_group", "snapshot_date"])[f"ret_{d}d"].transform("median")
    feats["rel_to_market_7d"] = feats["ret_7d"] - feats["market_ret_7d"]

    # AI-model release intensity (the project's hypothesis: hype moves prices)
    rel = releases.to_numpy(dtype="datetime64[ns]")
    dates = feats["snapshot_date"].to_numpy(dtype="datetime64[ns]")
    upto = np.searchsorted(rel, dates, side="right")
    for d in (30, 90):
        start = np.searchsorted(rel, dates - np.timedelta64(d, "D"), side="right")
        feats[f"releases_{d}d"] = (upto - start).astype(float)
    feats["days_since_llm_release"] = feats["days_since_llm_release"].astype(float)

    feats[LABEL] = np.where(feats[TARGET_RETURN].isna(), np.nan,
                            (feats[TARGET_RETURN] <= -DROP_THRESHOLD).astype(float))
    return feats.sort_values(KEYS).reset_index(drop=True)
