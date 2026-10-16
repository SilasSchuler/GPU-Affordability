"""Postgres helpers: connection, raw-schema DDL, ingestion run logging."""

from __future__ import annotations

import traceback
from datetime import date
from types import TracebackType

import psycopg2
from psycopg2.extensions import connection

from .config import SETTINGS

DDL = """
CREATE TABLE IF NOT EXISTS raw.gpu_deals (
    id            BIGSERIAL PRIMARY KEY,
    snapshot_date DATE        NOT NULL,
    source_file   TEXT        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload       JSONB       NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.epoch_models (
    id         BIGSERIAL PRIMARY KEY,
    load_date  DATE        NOT NULL,
    source     TEXT        NOT NULL,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload    JSONB       NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT        NOT NULL,
    mode          TEXT        NOT NULL,
    snapshot_date DATE,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT        NOT NULL,
    rows_loaded   INTEGER,
    message       TEXT
);

CREATE TABLE IF NOT EXISTS raw.source_versions (
    source       TEXT PRIMARY KEY,
    content_hash TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def get_conn() -> connection:
    conn = psycopg2.connect(
        host=SETTINGS.postgres_host,
        port=SETTINGS.postgres_port,
        dbname=SETTINGS.postgres_db,
        user=SETTINGS.postgres_user,
        password=SETTINGS.postgres_password,
    )
    conn.autocommit = False
    return conn


def ensure_schema(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def get_source_hash(conn: connection, source: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT content_hash FROM raw.source_versions WHERE source = %s",
            (source,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def set_source_hash(conn: connection, source: str, content_hash: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.source_versions (source, content_hash, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (source)
            DO UPDATE SET content_hash = EXCLUDED.content_hash,
                          updated_at   = EXCLUDED.updated_at
            """,
            (source, content_hash),
        )
    conn.commit()


class IngestionRun:
    """Context manager that records one pipeline run in raw.ingestion_log.

    Usage:
        with IngestionRun(conn, source="gpu_deals", mode="daily", snapshot_date=d) as run:
            ... do the work ...
            run.rows_loaded = n
            run.message = "optional note"
    On exception the run is marked 'failed' with the traceback as message.
    """

    def __init__(
        self,
        conn: connection,
        source: str,
        mode: str,
        snapshot_date: date | None = None,
    ) -> None:
        self.conn = conn
        self.source = source
        self.mode = mode
        self.snapshot_date = snapshot_date
        self.rows_loaded: int | None = None
        self.message: str | None = None
        self._log_id: int | None = None

    def __enter__(self) -> "IngestionRun":
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.ingestion_log (source, mode, snapshot_date, status)
                VALUES (%s, %s, %s, 'running')
                RETURNING id
                """,
                (self.source, self.mode, self.snapshot_date),
            )
            self._log_id = cur.fetchone()[0]
        self.conn.commit()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        status = "success" if exc_type is None else "failed"
        message = self.message
        if exc_val is not None:
            message = "".join(
                traceback.format_exception(exc_type, exc_val, exc_tb)
            )[-4000:]
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE raw.ingestion_log
                   SET finished_at = now(), status = %s, rows_loaded = %s, message = %s
                 WHERE id = %s
                """,
                (status, self.rows_loaded, message, self._log_id),
            )
        self.conn.commit()
        return False  # never swallow exceptions
