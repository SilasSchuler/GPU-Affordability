"""GPU deals -> staging.gpu_deals.

Justification: the Buy/Wait use case needs queryable deal rows with a
release context. The raw layer stores 45-field JSONB payloads; this table
selects the fields used for analysis, computes price per GB, and links each
deal to the latest AI model release on or before its snapshot date.

Operations: select+rename, cast strings to typed columns, validate rows,
deduplicate exact duplicate listings, compute price per GB, and join the
release calendar.

Validation: a row is rejected (not loaded) when title, link, source or price
is missing, or when price is not a plain number after stripping "$", "," and
spaces. A bad currency code rejects the row; a missing currency becomes USD.
A bad vram_gb becomes NULL (the deal is kept). If more than MAX_REJECT_SHARE
of the raw rows are rejected, the build raises and rolls back.

Rerunnable: TRUNCATE + INSERT for the full history, or DELETE + INSERT for
one snapshot date. Raw stays intact.

CLI: python -m src.transformation.gpu_deals build [--date YYYY-MM-DD]
[--min-rows N]. Without --date the full history is rebuilt. The build fails
and rolls back when fewer than --min-rows rows are loaded (0 disables it).
"""

from __future__ import annotations

import argparse
from datetime import date
import sys

from ..errors import TooFewRowsError
from ..ingestion.config import MIN_EXPECTED_ROWS
from ..ingestion.db import STAGING_SCHEMA_LOCK, get_conn
from .model_releases import build as build_model_releases

TARGET = "staging.gpu_deals"
MAX_REJECT_SHARE = 0.05

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


def _src_cte(where: str) -> str:
    """Shared CTE: `src` = all raw rows in scope as text, `valid` = rows that pass."""
    return rf"""
WITH src AS (
    SELECT d.snapshot_date,
           NULLIF(btrim(d.payload ->> 'title'), '')    AS title,
           d.payload ->> 'brand'                       AS brand,
           regexp_replace(d.payload ->> 'price', '[$,[:space:]]', '', 'g') AS price_txt,
           NULLIF(btrim(d.payload ->> 'currency'), '') AS currency,
           d.payload ->> 'condition'                   AS condition,
           NULLIF(btrim(d.payload ->> 'link'), '')     AS link,
           NULLIF(btrim(d.payload ->> 'source'), '')   AS source,
           d.payload ->> 'itemGroupType'               AS item_group_type,
           d.payload ->> 'chipset'                     AS chipset,
           NULLIF(btrim(d.payload ->> 'vram_gb'), '')  AS vram_txt
      FROM raw.gpu_deals d
     {where}
), valid AS (
    SELECT *
      FROM src
     WHERE title IS NOT NULL
       AND link IS NOT NULL
       AND source IS NOT NULL
       AND price_txt ~ '^\d{{1,8}}(\.\d+)?$'
       AND (currency IS NULL OR currency ~ '^[A-Za-z]{{3}}$')
)
"""


COUNT_TAIL = "SELECT (SELECT count(*) FROM src), (SELECT count(*) FROM valid);"


def _insert_sql(where: str) -> str:
    return _src_cte(where) + rf"""
INSERT INTO {TARGET} (
   title, brand, price, currency, condition, link, source,
   item_group_type, chipset, vram_gb, scraped_at,
   model_release_id, days_since_llm_release
)
SELECT DISTINCT
   v.title,
   v.brand,
   v.price_txt::NUMERIC(10,2),
   COALESCE(upper(v.currency), 'USD'),
   v.condition,
   v.link,
   v.source,
   v.item_group_type,
   v.chipset,
   CASE WHEN v.vram_txt ~ '^\d{{1,4}}(\.\d+)?$' THEN v.vram_txt::NUMERIC(6,2) END,
   v.snapshot_date,
   m.id,
   v.snapshot_date::date - m.release_date
FROM valid v
LEFT JOIN LATERAL (
   SELECT r.id, r.release_date
   FROM staging.model_releases r
   WHERE r.release_date <= v.snapshot_date::date
   ORDER BY r.release_date DESC, r.id DESC
   LIMIT 1
) m ON TRUE;
"""


WHERE_ALL = ""
WHERE_DAY = "WHERE d.snapshot_date = %(day)s"

DELETE_SQL_DAY = "DELETE FROM staging.gpu_deals WHERE scraped_at::date = %(day)s"


def _load(cur, where: str, params: dict | None) -> int:
    """Validate, guard against mass rejection, insert. Returns inserted rows."""
    cur.execute(_src_cte(where) + COUNT_TAIL, params)
    total, valid = cur.fetchone()
    rejected = total - valid
    print(f"gpu_deals: {rejected} of {total} raw rows rejected as invalid")
    if total and rejected / total > MAX_REJECT_SHARE:
        raise ValueError(
            f"{rejected} of {total} raw rows invalid "
            f"(> {MAX_REJECT_SHARE:.0%}); build aborted, staging unchanged"
        )
    cur.execute(_insert_sql(where), params)
    return cur.rowcount


def build_history(min_rows: int | None = None) -> int:
    """Rebuild staging.gpu_deals from the latest raw version."""
    build_model_releases()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (STAGING_SCHEMA_LOCK,))
            cur.execute(DDL)
            cur.execute(f"TRUNCATE {TARGET}")
            count = _load(cur, WHERE_ALL, None)
            # check before commit so the rollback keeps the old staging rows
            if min_rows is not None and count < min_rows:
                raise TooFewRowsError("transform staging.gpu_deals", count, min_rows)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()
    return count


def build(day: date, min_rows: int | None = None) -> int:
    """Rebuild staging.gpu_deals from the raw version for a specific day."""
    build_model_releases()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (STAGING_SCHEMA_LOCK,))
            cur.execute(DDL)
            cur.execute(DELETE_SQL_DAY, {"day": day})
            count = _load(cur, WHERE_DAY, {"day": day})
            # check before commit so the rollback keeps the old staging rows
            if min_rows is not None and count < min_rows:
                raise TooFewRowsError(
                    "transform staging.gpu_deals", count, min_rows, day
                )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()
    return count


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m src.transformation.gpu_deals")
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build", help="rebuild staging.gpu_deals from raw")
    b.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="snapshot date YYYY-MM-DD to rebuild; omit for a full-history rebuild",
    )
    b.add_argument(
        "--min-rows",
        type=int,
        default=MIN_EXPECTED_ROWS,
        help=f"fail and roll back if fewer rows are loaded (default {MIN_EXPECTED_ROWS}; 0 disables)",
    )
    args = parser.parse_args()
    min_rows = args.min_rows if args.min_rows > 0 else None
    if args.date is None:
        count = build_history(min_rows)
        print(f"gpu_deals: built {count} rows -> {TARGET}")
    else:
        count = build(args.date, min_rows)
        print(f"gpu_deals: built {count} rows for {args.date} -> {TARGET}")


if __name__ == "__main__":
    sys.exit(main())
