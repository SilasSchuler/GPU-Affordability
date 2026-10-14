"""Ingestion pipelines for the GPU-Affordability project.

Lands source data as immutable raw copies in MinIO (bucket "raw") and as
queryable JSONB rows in the Postgres "raw" schema. No cleaning or business
transformation happens here — that is the W14 transform phase.
"""
