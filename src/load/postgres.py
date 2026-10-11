from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from src.config import get_settings, resolve_run_id
from src.schema import CURATED_COLUMNS

CURATED_TABLE = ("curated", "sales_order_lines")
BENCH_TABLE = ("audit", "benchmark_sales_order_lines")


def get_connection():
    return psycopg2.connect(**get_settings().pg_kwargs())


def ensure_schema(conn) -> None:
    ddl = (get_settings().project_root / "sql" / "init" / "01_schema.sql").read_text(encoding="utf-8")
    with conn, conn.cursor() as cur:
        cur.execute(ddl)


def _py(v):
    if v is None or (not isinstance(v, (list, dict)) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    return v


def upsert_dataframe(conn, df: pd.DataFrame, table: tuple = CURATED_TABLE) -> dict:
    """INSERT ... ON CONFLICT (order_id) DO UPDATE ... WHERE hash differs -> unchanged rows are not touched."""
    df = df[CURATED_COLUMNS].drop_duplicates("order_id", keep="last")
    if df.empty:
        return {"total": 0, "inserted": 0, "updated": 0, "unchanged": 0}
    rows = [tuple(_py(v) for v in rec) for rec in df.itertuples(index=False, name=None)]
    update_cols = [c for c in CURATED_COLUMNS if c != "order_id"]
    query = sql.SQL(
        "INSERT INTO {tbl} AS t ({cols}) VALUES %s "
        "ON CONFLICT (order_id) DO UPDATE SET {sets} "
        "WHERE t.record_hash IS DISTINCT FROM EXCLUDED.record_hash "
        "RETURNING (xmax = 0) AS inserted"
    ).format(
        tbl=sql.Identifier(*table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in CURATED_COLUMNS),
        sets=sql.SQL(", ").join(sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in update_cols),
    )
    with conn, conn.cursor() as cur:
        result = execute_values(cur, query, rows, page_size=1000, fetch=True)
    inserted = sum(1 for r in result if r[0])
    updated = len(result) - inserted
    return {"total": len(rows), "inserted": inserted, "updated": updated, "unchanged": len(rows) - len(result)}


def load_curated(run_id: str | None) -> dict:
    s = get_settings()
    rid = resolve_run_id(run_id, s.curated_dir)
    path = s.curated_dir / f"run_id={rid}" / "sales_order_lines.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Curated data not found: {path} (run extract + transform first)")
    df = pd.read_parquet(path)
    conn = get_connection()
    try:
        ensure_schema(conn)
        stats = upsert_dataframe(conn, df)
    finally:
        conn.close()
    return {"run_id": rid, **stats}


def load_partition(run_id: str | None, year: int, month: int) -> dict:
    from src.load.partitions import read_partition, write_partitioned  # local import: keeps module light

    s = get_settings()
    rid = resolve_run_id(run_id, s.curated_dir)
    curated_path = s.curated_dir / f"run_id={rid}" / "sales_order_lines.parquet"
    if not curated_path.exists():
        raise FileNotFoundError(f"Curated data not found: {curated_path}")
    write_partitioned(pd.read_parquet(curated_path))
    part = read_partition(year, month)  # reads ONLY that partition directory
    conn = get_connection()
    try:
        ensure_schema(conn)
        stats = upsert_dataframe(conn, part)
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit.partition_loads (pipeline_run_id, order_year, order_month, rows_read, "
                "rows_inserted, rows_updated, rows_unchanged, loaded_at_utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (rid, year, month, stats["total"], stats["inserted"], stats["updated"], stats["unchanged"],
                 datetime.now(timezone.utc)))
    finally:
        conn.close()
    return {"run_id": rid, "order_year": year, "order_month": month, **stats}