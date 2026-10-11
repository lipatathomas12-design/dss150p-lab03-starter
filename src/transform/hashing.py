from __future__ import annotations

import hashlib

import pandas as pd

from src.schema import HASH_COLUMNS, MONEY_COLUMNS, PCT_COLUMNS


def _fmt(col: str, v) -> str:
    if v is None or (not isinstance(v, (list, dict)) and pd.isna(v)):
        return ""
    if col in MONEY_COLUMNS:
        return f"{float(v):.2f}"
    if col in PCT_COLUMNS:
        return f"{float(v):.4f}"
    if isinstance(v, pd.Timestamp):
        return v.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S.%f") if v.tzinfo else v.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return str(v).strip()


def compute_record_hash(row) -> str:
    """Deterministic over business content only; never changes because the run time/id changed."""
    payload = "\x1f".join(_fmt(c, row[c]) for c in HASH_COLUMNS)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    