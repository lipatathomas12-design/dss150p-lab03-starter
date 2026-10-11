from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src.config import get_settings
from src.schema import CURATED_COLUMNS
from src.transform.hashing import compute_record_hash
from src.transform.quarantine import make_quarantine


def build_curated_frames(orders, customers, products, run_id: str):
    cust = customers[["customer_id", "email", "city"]].rename(
        columns={"email": "customer_email", "city": "customer_city"})
    prod = products[["product_id", "product_name", "category_name", "category_department", "unit_price"]]
    m = orders.merge(cust, on="customer_id", how="left", indicator="_c")
    m = m.rename(columns={"_c": "_cust"}).merge(prod, on="product_id", how="left", indicator="_p")

    no_c, no_p = m["_cust"] == "left_only", m["_p"] == "left_only"
    orphan = no_c | no_p
    reason = pd.Series("orphan_customer_and_product", index=m.index)
    reason[no_c & ~no_p] = "orphan_customer"
    reason[~no_c & no_p] = "orphan_product"

    q_frames = []
    for r in sorted(reason[orphan].unique()):
        rows = m[orphan & (reason == r)][["order_id", "customer_id", "product_id", "quantity", "status"]]
        q_frames.append(make_quarantine(rows, "orders", "order_id", r, run_id))
    quarantine = pd.concat(q_frames, ignore_index=True) if q_frames else None

    v = m.loc[~orphan].copy()
    v["gross_amount"] = (v["quantity"] * v["unit_price"]).round(2)
    v["discount_amount"] = (v["gross_amount"] * v["discount_pct"]).round(2)
    v["net_amount"] = (v["gross_amount"] - v["discount_amount"]).round(2)
    v["order_year"] = v["order_timestamp"].dt.year.astype("int64")
    v["order_month"] = v["order_timestamp"].dt.month.astype("int64")
    v["source_updated_at"] = v["updated_at"]
    v["pipeline_run_id"] = run_id
    v["processed_at_utc"] = datetime.now(timezone.utc)
    v["record_hash"] = v.apply(compute_record_hash, axis=1)
    return v[CURATED_COLUMNS].reset_index(drop=True), quarantine


def build_curated(run_id: str):
    s = get_settings()
    stg = s.staging_dir / f"run_id={run_id}"
    if not stg.exists():
        raise FileNotFoundError(f"Staging data not found: {stg}")
    curated, quarantine = build_curated_frames(
        pd.read_parquet(stg / "orders.parquet"), pd.read_parquet(stg / "customers.parquet"),
        pd.read_parquet(stg / "products.parquet"), run_id)
    out = s.curated_dir / f"run_id={run_id}"
    out.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(out / "sales_order_lines.parquet", index=False)
    return curated, quarantine