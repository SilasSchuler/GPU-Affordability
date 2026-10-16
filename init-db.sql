CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;

-- Backend-store database for the MLflow service (model-registry).
CREATE DATABASE mlflow;

-- Orchestration: Airflow metadata database + user (services: airflow-*).
-- Local development credentials, matching docker-compose.yml.
CREATE ROLE airflow LOGIN PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
