CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;

-- Backend-store database for the MLflow service (model-registry).
CREATE DATABASE mlflow;
