"""Environment-driven configuration.

Works in both run modes:
- host (`uv run ...`): loads .env from the repo root, services reached via
  the localhost port mappings (127.0.0.1:5432 / 127.0.0.1:9000).
- container (`docker compose run ingestion ...`): compose injects the env
  vars; services reached via the docker network names.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
GPU_DATA_DIR = DATA_DIR / "GPU"
EPOCH_CSV = DATA_DIR / "epoch_ai_models_release" / "Epoch Database - Notable Models.csv"

load_dotenv(REPO_ROOT / ".env")

RAW_BUCKET = "raw"

GPU_REPO_RAW = "https://raw.githubusercontent.com/HardwareDealsCo/gpu-deals/main"


@dataclass(frozen=True)
class Settings:
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "gpu")
    postgres_user: str = os.getenv("POSTGRES_USER", "sharedUser")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "")

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_admin: str = os.getenv("MINIO_ADMIN", "minioadmin")
    minio_secret: str = os.getenv("MINIO_SECRET", "")


SETTINGS = Settings()
