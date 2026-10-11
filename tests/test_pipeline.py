import pandas as pd

from src.transform.curated import build_curated_frames
from src.transform.hashing import compute_record_hash
from src.transform.staging import stage_customers, stage_orders, stage_products

RULES = {"min_quantity": 1, "max_quantity": 20, "allowed_statuses": ("PENDING", "DELIVERED")}


def _orders(rows):
    cols = ["order_id", "customer_id", "product_id", "quantity", "discount_pct", "status",
            "order_timestamp", "updated_at"]
    return pd.DataFrame(rows, columns=cols, dtype=str)


def test_customers_dedupe_latest_normalize_and_quarantine():
    raw = pd.DataFrame({
        "customer_id": ["1", "1", "2", None],
        "email": [" OLD@X.COM ", " New@X.com ", None, "a@b.c"],
        "city": [" manila ", " quezon CITY", "cebu", "x"],
        "updated_at": ["2026-01-01", "2026-02-01", "2026-01-01", "2026-01-01"]})
    clean, q = stage_customers(raw, "r1")
    assert len(clean) == 2
    one = clean[clean.customer_id == "1"].iloc[0]
    assert one.email == "new@x.com" and one.city == "Quezon City"
    assert clean[clean.customer_id == "2"].iloc[0].email_missing
    assert list(q.reason) == ["missing_business_key"]


def test_products_flatten_and_negative_price_quarantined():
    raw = pd.json_normalize([
        {"product_id": "p1", "price": "10.5", "updated_at": "2026-01-01",
         "category": {"name": "Toys", "department": "Kids"}},
        {"product_id": "p2", "price": "-3", "updated_at": "2026-01-01",
         "category": {"name": "Toys", "department": "Kids"}}], sep=".")
    clean, q = stage_products(raw, "r1")
    assert list(clean.product_id) == ["p1"] and clean.category_name[0] == "Toys"
    assert list(q.reason) == ["invalid_price"]


def test_orders_rules():
    raw = _orders([
        ["o1", "c1", "p1", "2", "0.1", "delivered", "2026-01-05T10:00:00Z", "2026-01-05"],
        ["o2", "c1", "p1", "99", "0", "DELIVERED", "2026-01-05T10:00:00Z", "2026-01-05"],
        ["o3", "c1", "p1", "1", "0", "WEIRD", "2026-01-05T10:00:00Z", "2026-01-05"],
        ["o4", "c1", "p1", "1", "0", "PENDING", "not-a-date", "2026-01-05"],
        [None, "c1", "p1", "1", "0", "PENDING", "2026-01-05T10:00:00Z", "2026-01-05"]])
    clean, q = stage_orders(raw, "r1", RULES)
    assert list(clean.order_id) == ["o1"]
    assert sorted(q.reason) == ["invalid_order_timestamp", "invalid_quantity", "invalid_status", "missing_business_key"]


def _curated(orders_rows):
    orders, _ = stage_orders(_orders(orders_rows), "r1", RULES)
    cust, _ = stage_customers(pd.DataFrame({"customer_id": ["c1"], "email": ["a@b.c"], "city": ["manila"],
                                            "updated_at": ["2026-01-01"]}), "r1")
    prod, _ = stage_products(pd.DataFrame({"product_id": ["p1"], "price": ["100"], "updated_at": ["2026-01-01"]}),
                             "r1")
    return build_curated_frames(orders, cust, prod, "r1")


def test_curated_amounts_orphans_and_hash():
    cur, q = _curated([
        ["o1", "c1", "p1", "2", "0.1", "DELIVERED", "2026-01-05T10:00:00Z", "2026-01-05"],
        ["o2", "c9", "p1", "1", "0", "DELIVERED", "2026-01-05T10:00:00Z", "2026-01-05"]])
    row = cur.iloc[0]
    assert (row.gross_amount, row.discount_amount, row.net_amount) == (200.0, 20.0, 180.0)
    assert (row.order_year, row.order_month) == (2026, 1)
    assert list(q.reason) == ["orphan_customer"]


def test_hash_ignores_run_and_time_but_not_business_fields():
    cur, _ = _curated([["o1", "c1", "p1", "2", "0.1", "DELIVERED", "2026-01-05T10:00:00Z", "2026-01-05"]])
    row = cur.iloc[0].copy()
    base = compute_record_hash(row)
    row["pipeline_run_id"], row["processed_at_utc"] = "other", pd.Timestamp("2030-01-01", tz="UTC")
    assert compute_record_hash(row) == base
    row["status"] = "PENDING"
    assert compute_record_hash(row) != base
    row["status"], row["discount_amount"] = "DELIVERED", 99.0
    assert compute_record_hash(row) != base