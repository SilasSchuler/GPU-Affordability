"""GPU deals -> staging.gpu_deals.

Justification: the pitch defines a release calendar (model_name,
organization, release_date) as input to the Buy/Wait forecast — the raw
table (45 JSONB fields) can't serve that directly.

Operations: select+rename, cast the date string to DATE, drop unparseable
dates, deduplicate, and apply the business rule release_date >= 2020
(the filter that used to be applied manually in the Epoch web UI).

Rerunnable: TRUNCATE + INSERT from the latest raw version. Raw stays intact.
"""

from __future__ import annotations

import sys

from ..ingestion.db import get_conn

TARGET = "staging.gpu_deals"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
	ID BIGSERIAL PRIMARY KEY,
	TITLE TEXT NOT NULL,
	BRAND TEXT,
	PRICE NUMERIC(10, 2) NOT NULL,
	CURRENCY CHAR(3) NOT NULL DEFAULT 'USD',
	CONDITION TEXT,
	LINK TEXT NOT NULL,
	SOURCE TEXT NOT NULL,
	ITEM_GROUP_TYPE TEXT,
	CHIPSET TEXT,
	VRAM_GB NUMERIC(6,2),
	PRICE_PER_GB NUMERIC(10, 2) GENERATED ALWAYS AS (PRICE / NULLIF(VRAM_GB, 0)) STORED,
	SCRAPED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	MODEL_RELEASE_ID BIGINT REFERENCES staging.model_releases(ID),
	DAYS_SINCE_LLM_RELEASE SMALLINT
);
"""

BUILD_SQL = f"""
INSERT INTO {TARGET} (
   title, brand, price, currency, condition, link, source,
   item_group_type, chipset, vram_gb, scraped_at,
   model_release_id, days_since_llm_release
)
SELECT DISTINCT
   d.payload ->> 'title',
   d.payload ->> 'brand',
   (d.payload ->> 'price')::NUMERIC(10,2),
   d.payload ->> 'currency',
   d.payload ->> 'condition',
   d.payload ->> 'link',
   d.payload ->> 'source',
   d.payload ->> 'itemGroupType',
   d.payload ->> 'chipset',
   (d.payload ->> 'vram_gb')::NUMERIC(6,2),
   d.snapshot_date,
   m.id,
   d.snapshot_date::date - m.release_date
FROM raw.gpu_deals d
LEFT JOIN LATERAL (
   SELECT r.id, r.release_date
   FROM staging.model_releases r
   WHERE r.release_date <= d.snapshot_date::date
   ORDER BY r.release_date DESC, r.id DESC
   LIMIT 1
) m ON TRUE
WHERE d.payload ->> 'title' IS NOT NULL;
"""


def build() -> int:
    """Rebuild staging.gpu_deals from the latest raw version."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(f"TRUNCATE {TARGET}")
            cur.execute(BUILD_SQL)
            count = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return count


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] != "build":
        print("usage: python -m src.transformation.gpu_deals build")
        sys.exit(1)
    count = build()
    print(f"gpu_deals: built {count} rows -> {TARGET}")


if __name__ == "__main__":
    sys.exit(main())
