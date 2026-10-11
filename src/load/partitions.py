from __future__ import annotations

import shutil

import pandas as pd

from src.config import get_settings


def write_partitioned(df: pd.DataFrame) -> str:
    out = get_settings().partition_dir
    if out.exists():
        shutil.rmtree(out)  # rebuild cleanly so reruns never duplicate files/rows
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, partition_cols=["order_year", "order_month"], index=False)
    return str(out)


def read_partition(year: int, month: int) -> pd.DataFrame:
    folder = get_settings().partition_dir / f"order_year={year}" / f"order_month={month}"
    if not folder.exists():
        raise FileNotFoundError(f"Partition not found: {folder}")
    df = pd.read_parquet(folder)  # only this folder is touched (partition pruning by path)
    df["order_year"], df["order_month"] = year, month  # partition columns live in the path, not the files
    ts = df["order_timestamp"]
    if not ((ts.dt.year == year) & (ts.dt.month == month)).all():
        raise ValueError(f"Partition {year}-{month:02d} contains rows from other months")
    return df


def tree_listing() -> str:
    root = get_settings().partition_dir
    lines = [str(root)]
    for p in sorted(root.rglob("*")):
        lines.append("  " * (len(p.relative_to(root).parts)) + p.name)
    return "\n".join(lines)