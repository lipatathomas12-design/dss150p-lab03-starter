from __future__ import annotations

import shutil

from src.config import get_settings, new_run_id, normalize_run_id

SOURCE_FILES = ["customers.csv", "products.json", "orders.csv"]


def extract_sources(run_id: str | None = None) -> str:
    s = get_settings()
    rid = new_run_id() if run_id in (None, "latest") else normalize_run_id(run_id)
    dest = s.raw_dir / f"run_id={rid}"
    dest.mkdir(parents=True, exist_ok=True)
    for name in SOURCE_FILES:
        src = s.source_dir / name
        if not src.exists():
            raise FileNotFoundError(f"Source file not found: {src}")
        shutil.copy2(src, dest / name)  # byte-for-byte copy, source is never modified
    return rid