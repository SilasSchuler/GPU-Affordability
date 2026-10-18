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

import sys

from ..ingestion.db import get_conn

TARGET = "staging.model_releases"

DDL = f"""
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
            cur.execute(DDL)
            cur.execute(BUILD_SQL)
            count = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return count


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] != "build":
        print("usage: python -m src.transformation.model_releases build")
        sys.exit(1)
    count = build()
    print(f"model_releases: built {count} rows -> {TARGET}")


if __name__ == "__main__":
    sys.exit(main())
