"""GPU deals -> staging.gpu_deals.

Justification: the Buy/Wait use case needs queryable deal rows with a
release context. The raw layer stores 45-field JSONB payloads; this table
selects the fields used for analysis, computes price per GB, and links each
deal to the latest AI model release on or before its snapshot date.

Operations: select+rename, cast strings to typed columns, deduplicate exact
duplicate listings, compute price per GB, and join the release calendar.

Rerunnable: TRUNCATE + INSERT for the full history, or DELETE + INSERT for
one snapshot date. Raw stays intact.
"""

from __future__ import annotations

from datetime import date
import sys

from ..ingestion.db import STAGING_SCHEMA_LOCK, get_conn
from .model_releases import build as build_model_releases

TARGET = "staging.gpu_deals"

DDL = f"""
CREATE SCHEMA IF NOT EXISTS staging;

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

BUILD_SQL_ALL = f"""
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

BUILD_SQL_DAY = f"""
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
WHERE d.payload ->> 'title' IS NOT NULL
    AND d.snapshot_date = %(day)s;
"""

DELETE_SQL_DAY = "DELETE FROM staging.gpu_deals WHERE scraped_at::date = %(day)s"



def build_history() -> int:
    """Rebuild staging.gpu_deals from the latest raw version."""
    build_model_releases()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (STAGING_SCHEMA_LOCK,))
            cur.execute(DDL)
            cur.execute(f"TRUNCATE {TARGET}")
            cur.execute(BUILD_SQL_ALL)
            count = cur.rowcount
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()
    return count

def build(day: date) -> int:
    """Rebuild staging.gpu_deals from the raw version for a specific day."""
    build_model_releases()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (STAGING_SCHEMA_LOCK,))
            cur.execute(DDL)
            cur.execute(DELETE_SQL_DAY, {"day": day})
            cur.execute(BUILD_SQL_DAY, {"day": day})
            count = cur.rowcount
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()
    return count


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] != "build":
        print("usage: python -m src.transformation.gpu_deals build")
        sys.exit(1)
    count = build_history()
    print(f"gpu_deals: built {count} rows -> {TARGET}")


if __name__ == "__main__":
    sys.exit(main())
