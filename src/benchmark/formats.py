from __future__ import annotations

import json
import platform
import statistics
import sys
import time

import pandas as pd

from src.config import get_settings, resolve_run_id
from src.load.partitions import tree_listing, write_partitioned
from src.load.postgres import BENCH_TABLE, ensure_schema, get_connection, upsert_dataframe

STATUS = "DELIVERED"


def _time(fn, repeats):
    times, result = [], None
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), result


def _pg_df(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def run_benchmark(run_id: str | None, repeats: int = 5) -> dict:
    s = get_settings()
    rid = resolve_run_id(run_id, s.curated_dir)
    df = pd.read_parquet(s.curated_dir / f"run_id={rid}" / "sales_order_lines.parquet")
    mat = s.benchmark_dir / "materialized"
    mat.mkdir(parents=True, exist_ok=True)
    csv_p, jsonl_p, pq_p = mat / "curated.csv", mat / "curated.jsonl", mat / "curated.parquet"
    rows = []

    def file_format(name, path, write, read_full, read_filtered):
        w, _ = _time(lambda: write(path), repeats)
        f, full = _time(lambda: read_full(path), repeats)
        q, flt = _time(lambda: read_filtered(path), repeats)
        rows.append(dict(format=name, size_bytes=path.stat().st_size, size_note="file size",
                         write_median_s=w, full_read_median_s=f, filtered_read_median_s=q,
                         full_rows=len(full), filtered_rows=len(flt), repeats=repeats))

    file_format("CSV", csv_p, lambda p: df.to_csv(p, index=False), pd.read_csv,
                lambda p: (lambda d: d[d["status"] == STATUS])(pd.read_csv(p)))
    file_format("JSONL", jsonl_p, lambda p: df.to_json(p, orient="records", lines=True, date_format="iso"),
                lambda p: pd.read_json(p, lines=True),
                lambda p: (lambda d: d[d["status"] == STATUS])(pd.read_json(p, lines=True)))
    file_format("Parquet", pq_p, lambda p: df.to_parquet(p, index=False, compression="snappy"), pd.read_parquet,
                lambda p: pd.read_parquet(p, filters=[("status", "==", STATUS)]))

    conn = get_connection()
    try:
        ensure_schema(conn)
        tbl = ".".join(BENCH_TABLE)

        def pg_write():
            with conn, conn.cursor() as cur:
                cur.execute(f"TRUNCATE {tbl}")
            return upsert_dataframe(conn, df, BENCH_TABLE)

        w, _ = _time(pg_write, repeats)
        f, full = _time(lambda: _pg_df(conn, f"SELECT * FROM {tbl}"), repeats)
        q, flt = _time(lambda: _pg_df(conn, f"SELECT * FROM {tbl} WHERE status = '{STATUS}'"), repeats)
        with conn.cursor() as cur:
            cur.execute(f"SELECT pg_total_relation_size('{tbl}'), version()")
            size, pg_version = cur.fetchone()
        rows.append(dict(format="PostgreSQL", size_bytes=size, size_note="pg_total_relation_size (table+indexes)",
                         write_median_s=w, full_read_median_s=f, filtered_read_median_s=q,
                         full_rows=len(full), filtered_rows=len(flt), repeats=repeats))
    finally:
        conn.close()

    write_partitioned(df)
    (s.benchmark_dir / "partition_tree.txt").write_text(tree_listing(), encoding="utf-8")

    result = pd.DataFrame(rows)
    result.to_csv(s.benchmark_dir / "benchmark_results.csv", index=False)
    env = {"python": sys.version, "platform": platform.platform(), "machine": platform.machine(),
           "pandas": pd.__version__, "postgres": pg_version, "run_id": rid, "curated_rows": len(df)}
    (s.benchmark_dir / "benchmark_environment.json").write_text(json.dumps(env, indent=2), encoding="utf-8")
    if result["full_rows"].nunique() != 1 or result["filtered_rows"].nunique() != 1:
        raise ValueError(f"Row counts differ across formats:\n{result[['format','full_rows','filtered_rows']]}")
    return {"run_id": rid, "results_csv": str(s.benchmark_dir / "benchmark_results.csv"), "rows": rows}