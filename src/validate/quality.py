from __future__ import annotations

import pandas as pd

from src.config import get_settings, resolve_run_id
from src.errors import DataQualityError


def run_quality_checks(run_id: str | None, year: int | None = None, month: int | None = None) -> dict:
    from src.load.postgres import get_connection  # DB read only

    s = get_settings()
    rid = resolve_run_id(run_id, s.curated_dir)
    df = pd.read_parquet(s.curated_dir / f"run_id={rid}" / "sales_order_lines.parquet")
    if year is not None and month is not None:
        df = df[(df["order_year"] == year) & (df["order_month"] == month)]

    failures, passed = [], []

    def check(name: str, bad_count: int):
        (failures if bad_count else passed).append(f"{name}: {bad_count} bad rows" if bad_count else name)

    check("curated_not_empty", int(df.empty))
    check("null_business_key", int(df["order_id"].isna().sum()))
    check("duplicate_business_key", int(df["order_id"].duplicated().sum()))
    check("invalid_status", int((~df["status"].isin(s.allowed_statuses)).sum()))
    check("negative_amounts", int(((df[["gross_amount", "discount_amount", "net_amount"]] < 0).any(axis=1)).sum()))
    check("gross_amount_mismatch", int(((df["quantity"] * df["unit_price"] - df["gross_amount"]).abs() > 0.011).sum()))
    check("net_amount_mismatch",
          int(((df["gross_amount"] - df["discount_amount"] - df["net_amount"]).abs() > 0.011).sum()))
    audit_cols = ["source_updated_at", "pipeline_run_id", "processed_at_utc", "record_hash"]
    check("null_audit_columns", int(df[audit_cols].isna().any(axis=1).sum()))

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM curated.sales_order_lines")
            total, distinct = cur.fetchone()
            check("postgres_duplicate_order_id", int(total - distinct))
            cur.execute("SELECT order_id, record_hash FROM curated.sales_order_lines WHERE order_id = ANY(%s)",
                        (df["order_id"].tolist(),))
            db = dict(cur.fetchall())
        expected = dict(zip(df["order_id"], df["record_hash"]))
        not_loaded = sum(1 for k, h in expected.items() if db.get(k, "").strip() != h)
        check("postgres_rows_match_curated", not_loaded)
    finally:
        conn.close()

    report = {"run_id": rid, "rows_checked": len(df), "passed": passed, "failed": failures}
    if failures:
        raise DataQualityError(f"Validation failed for run {rid}: {failures}")
    return report