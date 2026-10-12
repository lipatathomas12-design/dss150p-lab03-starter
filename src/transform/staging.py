"""Staging layer: typing, normalization, latest-version dedup, source-level validity checks."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd

from src.config import get_settings
from src.transform.quarantine import combine_quarantine, make_quarantine


def _ts(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, utc=True, errors="coerce", format="mixed")
    except (ValueError, TypeError):
        return pd.to_datetime(series, utc=True, errors="coerce")


def _clean_str(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    return s.mask(s == "")


def _drop_bad(df, raw, bad, source, key_col, reason, run_id, found):
    mask = pd.Series(bad, index=df.index).fillna(True).astype(bool)
    if mask.any():
        found.append(make_quarantine(raw.loc[df.index[mask]], source, key_col, reason, run_id))
    return df.loc[~mask]


def _dedupe_latest(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Keep the row with the greatest updated_at; ties -> the later row in the file (deterministic)."""
    df = df.copy()
    df["_pos"] = range(len(df))
    df = df.sort_values(["updated_at", "_pos"], kind="mergesort").drop_duplicates(key, keep="last")
    return df.drop(columns="_pos")


def _finish(df: pd.DataFrame, key: str, run_id: str) -> pd.DataFrame:
    df = df.sort_values(key, kind="mergesort").reset_index(drop=True)
    df["pipeline_run_id"] = run_id
    df["staged_at_utc"] = datetime.now(timezone.utc)
    return df


def stage_customers(raw: pd.DataFrame, run_id: str):
    found, df = [], raw.copy()
    df["customer_id"] = _clean_str(df["customer_id"])
    df["updated_at"] = _ts(df["updated_at"])
    df = _drop_bad(df, raw, df["customer_id"].isna(), "customers", "customer_id", "missing_business_key", run_id, found)
    df = _drop_bad(df, raw, df["updated_at"].isna(), "customers", "customer_id", "invalid_updated_at", run_id, found)
    df = _dedupe_latest(df, "customer_id")
    df["email"] = _clean_str(df["email"]).str.lower()
    df["email_missing"] = df["email"].isna()  # missing email stays visible as a quality condition
    df["city"] = _clean_str(df["city"]).str.title()
    return _finish(df, "customer_id", run_id), combine_quarantine(found)


def stage_products(raw: pd.DataFrame, run_id: str):
    found, df = [], raw.copy()
    df = df.rename(columns={"category.name": "category_name", "category.department": "category_department",
                            "name": "product_name"})
    for col in ("category_name", "category_department", "product_name"):
        if col not in df.columns:
            df[col] = pd.NA
    price_col = next((c for c in ("price", "unit_price") if c in df.columns), None)
    if price_col is None:
        raise ValueError("products source has no 'price' or 'unit_price' column")
    df["product_id"] = _clean_str(df["product_id"])
    df["updated_at"] = _ts(df["updated_at"])
    df = _drop_bad(df, raw, df["product_id"].isna(), "products", "product_id", "missing_business_key", run_id, found)
    df = _drop_bad(df, raw, df["updated_at"].isna(), "products", "product_id", "invalid_updated_at", run_id, found)
    df = _dedupe_latest(df, "product_id")
    price = pd.to_numeric(df[price_col], errors="coerce")
    df = _drop_bad(df, raw, price.isna() | (price < 0), "products", "product_id", "invalid_price", run_id, found)
    df["unit_price"] = pd.to_numeric(df[price_col])
    if price_col != "unit_price":
        df = df.drop(columns=price_col)
    return _finish(df, "product_id", run_id), combine_quarantine(found)


def stage_orders(raw: pd.DataFrame, run_id: str, rules: dict):
    found, df = [], raw.copy()
    for col in ("order_id", "customer_id", "product_id"):
        df[col] = _clean_str(df[col])
    df["order_timestamp"] = _ts(df["order_timestamp"])
    df["updated_at"] = _ts(df["updated_at"])
    src = "orders"
    df = _drop_bad(df, raw, df["order_id"].isna(), src, "order_id", "missing_business_key", run_id, found)
    df = _drop_bad(df, raw, df["order_timestamp"].isna(), src, "order_id", "invalid_order_timestamp", run_id, found)
    df = _drop_bad(df, raw, df["updated_at"].isna(), src, "order_id", "invalid_updated_at", run_id, found)
    df = _dedupe_latest(df, "order_id")

    qty = pd.to_numeric(df["quantity"], errors="coerce")
    bad_qty = qty.isna() | (qty < rules["min_quantity"]) | (qty > rules["max_quantity"]) | (qty != qty.round())
    df = _drop_bad(df, raw, bad_qty, src, "order_id", "invalid_quantity", run_id, found)
    df["quantity"] = pd.to_numeric(df["quantity"]).astype("int64")

    df["status"] = df["status"].astype("string").str.strip().str.upper()
    df = _drop_bad(df, raw, ~df["status"].isin(list(rules["allowed_statuses"])), src, "order_id",
                   "invalid_status", run_id, found)

    if "discount_pct" not in df.columns:
        df["discount_pct"] = "0"
    disc = pd.to_numeric(df["discount_pct"], errors="coerce")
    bad_disc = (disc.isna() & df["discount_pct"].notna()) | (disc < 0) | (disc > 1)
    df = _drop_bad(df, raw, bad_disc, src, "order_id", "invalid_discount_pct", run_id, found)
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0.0)
    return _finish(df, "order_id", run_id), combine_quarantine(found)


def _read_products_json(path) -> pd.DataFrame:
    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:  # JSON Lines fallback
        data = [json.loads(line) for line in text.splitlines() if line.strip()]
    if isinstance(data, dict):
        data = next((data[k] for k in ("products", "items", "data") if k in data), list(data.values()))
    return pd.json_normalize(data, sep=".")


def build_staging(run_id: str) -> dict:
    s = get_settings()
    raw_dir = s.raw_dir / f"run_id={run_id}"
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw snapshot not found: {raw_dir} (run extract first)")
    raw_c = pd.read_csv(raw_dir / "customers.csv", dtype=str, encoding="utf-8-sig")
    raw_p = _read_products_json(raw_dir / "products.json")
    raw_o = pd.read_csv(raw_dir / "orders.csv", dtype=str, encoding="utf-8-sig")
    for df in (raw_c, raw_o):
        df.columns = [c.strip() for c in df.columns]

    cust, q_c = stage_customers(raw_c, run_id)
    prod, q_p = stage_products(raw_p, run_id)
    orde, q_o = stage_orders(raw_o, run_id, {
        "min_quantity": s.min_quantity, "max_quantity": s.max_quantity, "allowed_statuses": s.allowed_statuses})

    out = s.staging_dir / f"run_id={run_id}"
    out.mkdir(parents=True, exist_ok=True)
    cust.to_parquet(out / "customers.parquet", index=False)
    prod.to_parquet(out / "products.parquet", index=False)
    orde.to_parquet(out / "orders.parquet", index=False)
    return {
        "customers": cust, "products": prod, "orders": orde,
        "quarantine": combine_quarantine([q_c, q_p, q_o]),
        "raw_counts": {"customers": len(raw_c), "products": len(raw_p), "orders": len(raw_o)},
    }