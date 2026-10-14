# src/

`ingestion/` — W7 ingestion pipelines (Natalie). See README "Ingestion Pipeline".

- `gpu_deals.py` — GPU deals: `seed` (local files), `backfill` (GitHub archive), `daily` (current deals)
- `epoch_models.py` — Epoch AI CSV: `snapshot`
- `config.py` / `db.py` / `storage.py` — shared env config, Postgres, MinIO helpers

Written Airflow-ready: single entrypoint per source, date-parameterized,
idempotent. Scheduling is a W14 orchestration task.
