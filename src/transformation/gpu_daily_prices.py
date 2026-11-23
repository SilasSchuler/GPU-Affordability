"""staging.gpu_deals -> staging.gpu_daily_prices.

Justification: the Buy/Wait model predicts the price of a GPU *type*, not
of a single eBay listing (listings appear and vanish daily). This table
aggregates listings to one robust price per chipset and condition per day,
which is the time series the model is trained on.

Operations: normalise chipset casing ("Rx 580" = "RX 580"), map eBay
conditions to 'new' / 'used' (drops "for parts" listings), drop non-USD and
implausible prices, aggregate with the median (robust to outliers), keep
groups with at least MIN_LISTINGS listings.

Grain: one row per (chipset, condition_group, snapshot_date).

Rerunnable: TRUNCATE + INSERT from staging.gpu_deals.
"""

from __future__ import annotations

import sys

from ..ingestion.db import STAGING_SCHEMA_LOCK, get_conn

TARGET = "staging.gpu_daily_prices"
MIN_LISTINGS = 3

DDL = f"""
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS {TARGET} (
    chipset                TEXT        NOT NULL,
    condition_group        TEXT        NOT NULL,
    snapshot_date          DATE        NOT NULL,
    brand                  TEXT,
    n_listings             INTEGER     NOT NULL,
    median_price           NUMERIC(10, 2) NOT NULL,
    p25_price              NUMERIC(10, 2) NOT NULL,
    min_price              NUMERIC(10, 2) NOT NULL,
    vram_gb                NUMERIC(6, 2),
    median_price_per_gb    NUMERIC(10, 2),
    days_since_llm_release SMALLINT,
    built_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chipset, condition_group, snapshot_date)
);
"""

BUILD_SQL = f"""
INSERT INTO {TARGET} (
    chipset, condition_group, snapshot_date, brand, n_listings,
    median_price, p25_price, min_price, vram_gb, median_price_per_gb,
    days_since_llm_release
)
WITH deals AS (
    SELECT
        upper(btrim(chipset)) AS chipset,
        CASE
            WHEN condition = 'New' THEN 'new'
            WHEN condition IN ('Used', 'Open box', 'Certified refurbished') THEN 'used'
        END AS condition_group,
        scraped_at::date AS snapshot_date,
        brand,
        price,
        vram_gb,
        price_per_gb,
        days_since_llm_release
    FROM staging.gpu_deals
    WHERE chipset IS NOT NULL
      AND currency = 'USD'
      AND price BETWEEN 20 AND 20000
)
SELECT
    chipset,
    condition_group,
    snapshot_date,
    mode() WITHIN GROUP (ORDER BY brand),
    count(*),
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY price),
    percentile_cont(0.25) WITHIN GROUP (ORDER BY price),
    min(price),
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY vram_gb),
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY price_per_gb),
    max(days_since_llm_release)
FROM deals
WHERE condition_group IS NOT NULL
GROUP BY chipset, condition_group, snapshot_date
HAVING count(*) >= {MIN_LISTINGS};
"""


def build() -> int:
    """Rebuild staging.gpu_daily_prices from staging.gpu_deals."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (STAGING_SCHEMA_LOCK,))
            cur.execute(DDL)
            cur.execute(f"TRUNCATE {TARGET}")
            cur.execute(BUILD_SQL)
            count = cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return count


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] != "build":
        print("usage: python -m src.transformation.gpu_daily_prices build")
        sys.exit(1)
    count = build()
    print(f"gpu_daily_prices: built {count} rows -> {TARGET}")


if __name__ == "__main__":
    sys.exit(main())
