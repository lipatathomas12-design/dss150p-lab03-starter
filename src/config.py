from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(os.environ.get("PIPELINE_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
load_dotenv(PROJECT_ROOT / ".env")  # does not override variables already set (e.g. by docker compose)

REQUIRED_ENV = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]


@dataclass(frozen=True)
class Settings:
    project_root: Path
    source_dir: Path
    raw_dir: Path
    staging_dir: Path
    curated_dir: Path
    quarantine_dir: Path
    benchmark_dir: Path
    partition_dir: Path
    min_quantity: int
    max_quantity: int
    allowed_statuses: tuple

    def pg_kwargs(self) -> dict:
        missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
        if missing:
            raise EnvironmentError(f"Missing environment variables: {', '.join(missing)} (copy .env.example to .env)")
        return dict(
            host=os.environ["POSTGRES_HOST"], port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"], connect_timeout=10,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    with open(PROJECT_ROOT / "config" / "settings.yml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    p, r = cfg["paths"], cfg["rules"]
    path = lambda k: PROJECT_ROOT / p[k]  # noqa: E731
    return Settings(
        project_root=PROJECT_ROOT,
        source_dir=path("source_dir"), raw_dir=path("raw_dir"), staging_dir=path("staging_dir"),
        curated_dir=path("curated_dir"), quarantine_dir=path("quarantine_dir"),
        benchmark_dir=path("benchmark_dir"), partition_dir=path("partition_dir"),
        min_quantity=int(r["min_quantity"]), max_quantity=int(r["max_quantity"]),
        allowed_statuses=tuple(s.upper() for s in r["allowed_statuses"]),
    )


def normalize_run_id(run_id: str) -> str:
    """Make any run id (incl. Airflow's 'manual__2026-..T..+00:00') safe for folder names."""
    return re.sub(r"[^A-Za-z0-9_.=-]", "-", run_id)


def new_run_id() -> str:
    return "run_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def resolve_run_id(run_id: str | None, base_dir: Path) -> str:
    """'latest'/None -> most recently modified run_id=* folder in base_dir."""
    if run_id and run_id != "latest":
        return normalize_run_id(run_id)
    runs = sorted(Path(base_dir).glob("run_id=*"), key=lambda p: p.stat().st_mtime)
    if not runs:
        raise FileNotFoundError(f"No run_id=* folders in {base_dir}; run the earlier stage first")
    return runs[-1].name.split("=", 1)[1]