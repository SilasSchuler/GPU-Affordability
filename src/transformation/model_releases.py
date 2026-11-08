"""Release calendar -> staging.model_releases.

Justification: the pitch defines a release calendar (model_name,
organization, release_date) as input to the Buy/Wait forecast — the raw
table (45 JSONB fields) can't serve that directly.

Operations: select+rename, cast the date string to DATE, drop unparseable
dates, deduplicate, and apply the business rule release_date >= 2020
(the filter that used to be applied manually in the Epoch web UI).

Rerunnable: UPSERT from the latest raw version. Raw stays intact.
"""

from __future__ import annotations

import argparse
import sys

from ..ingestion.db import STAGING_SCHEMA_LOCK, get_conn

TARGET = "staging.model_releases"

DDL = f"""
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS {TARGET} (
    ID BIGSERIAL PRIMARY KEY,
    model_name   TEXT NOT NULL,
    organization TEXT,
    release_date DATE NOT NULL,
    built_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (model_name, release_date)
);
"""

BUILD_SQL = f"""
   INSERT INTO {TARGET} (model_name, organization, release_date)
   SELECT DISTINCT ON (payload->>'Model', (payload->>'Publication date')::date)
          payload->>'Model',
          payload->>'Organization',
          (payload->>'Publication date')::date
     FROM raw.epoch_models
    WHERE load_date = (SELECT max(load_date) FROM raw.epoch_models)
      AND payload->>'Model' IS NOT NULL
      AND payload->>'Publication date' ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$'
      AND (payload->>'Publication date')::date >= DATE '2020-01-01'
    ORDER BY payload->>'Model', (payload->>'Publication date')::date,
             payload->>'Organization'
   ON CONFLICT (model_name, release_date)
   DO UPDATE SET organization = EXCLUDED.organization,
                 built_at = now();
   """

def build() -> int:
    """Rebuild staging.model_releases from the latest raw version."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (STAGING_SCHEMA_LOCK,))
            cur.execute(DDL)
            cur.execute(BUILD_SQL)
            count = cur.rowcount
            if count == 0:
                # raise before commit: nothing changes, caller sees the failure
                raise ValueError(
                    "model_releases: 0 rows from the latest raw.epoch_models load "
                    "(table empty, or every row filtered out)"
                )
        conn.commit()
    finally:
        conn.close()
    return count


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m src.transformation.model_releases")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("build", help="rebuild staging.model_releases from raw")
    parser.parse_args()
    count = build()
    print(f"model_releases: built {count} rows -> {TARGET}")


if __name__ == "__main__":
    sys.exit(main())
