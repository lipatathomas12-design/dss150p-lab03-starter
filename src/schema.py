CURATED_COLUMNS = [
    "order_id", "customer_id", "product_id",
    "customer_email", "customer_city",
    "product_name", "category_name", "category_department",
    "quantity", "unit_price", "discount_pct", "status",
    "order_timestamp", "order_year", "order_month",
    "gross_amount", "discount_amount", "net_amount",
    "source_updated_at", "pipeline_run_id", "processed_at_utc", "record_hash",
]

# Business content only: NO run id, NO processed_at/staged_at, NO source_updated_at.
HASH_COLUMNS = [
    "order_id", "customer_id", "product_id",
    "customer_email", "customer_city",
    "product_name", "category_name", "category_department",
    "quantity", "unit_price", "discount_pct", "status", "order_timestamp",
    "gross_amount", "discount_amount", "net_amount",
]
MONEY_COLUMNS = {"unit_price", "gross_amount", "discount_amount", "net_amount"}
PCT_COLUMNS = {"discount_pct"}

QUARANTINE_COLUMNS = [
    "source", "record_key", "reason", "raw_record",
    "pipeline_run_id", "quarantined_at_utc",
]