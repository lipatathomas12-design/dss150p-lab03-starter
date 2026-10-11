from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd

from src.schema import QUARANTINE_COLUMNS


def _safe(v):
    if isinstance(v, (list, dict)):
        return v
    return None if pd.isna(v) else str(v)


def make_quarantine(raw_rows: pd.DataFrame, source: str, key_col: str, reason: str, run_id: str) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    records = raw_rows.to_dict("records")
    keys = [_safe(r.get(key_col)) for r in records]
    return pd.DataFrame({
        "source": source,
        "record_key": keys,
        "reason": reason,
        "raw_record": [json.dumps({k: _safe(v) for k, v in r.items()}, sort_keys=True, default=str) for r in records],
        "pipeline_run_id": run_id,
        "quarantined_at_utc": now,
    }, columns=QUARANTINE_COLUMNS)


def combine_quarantine(frames: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)
    return pd.concat(frames, ignore_index=True)