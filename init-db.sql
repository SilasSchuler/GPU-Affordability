-- GPU-Affordability: one-time database bootstrap.
-- Executed by the Postgres docker entrypoint ONLY on first start
-- (i.e. when the data directory is empty). To re-run, wipe postgres_data/.

-- Pipeline schemas in the project DB (POSTGRES_DB, default "gpu").
-- raw:     immutable landing zone written by the ingestion pipeline.
-- staging: reserved for the W14 transformation phase.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;

-- Backend-store database for the MLflow service (model-registry).
CREATE DATABASE mlflow;

-- W14 note: the Airflow stack (containers/airflow) expects its own
-- `airflow` database + user on this server. Add when orchestration lands:
--   CREATE ROLE airflow LOGIN PASSWORD 'airflow';
--   CREATE DATABASE airflow OWNER airflow;
