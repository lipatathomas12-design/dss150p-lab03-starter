CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS curated.sales_order_lines (
    order_id             TEXT PRIMARY KEY,
    customer_id          TEXT NOT NULL,
    product_id           TEXT NOT NULL,
    customer_email       TEXT,
    customer_city        TEXT,
    product_name         TEXT,
    category_name        TEXT,
    category_department  TEXT,
    quantity             INTEGER NOT NULL,
    unit_price           NUMERIC(12,2) NOT NULL,
    discount_pct         NUMERIC(6,4) NOT NULL,
    status               TEXT NOT NULL,
    order_timestamp      TIMESTAMPTZ NOT NULL,
    order_year           INTEGER NOT NULL,
    order_month          INTEGER NOT NULL,
    gross_amount         NUMERIC(14,2) NOT NULL,
    discount_amount      NUMERIC(14,2) NOT NULL,
    net_amount           NUMERIC(14,2) NOT NULL,
    source_updated_at    TIMESTAMPTZ NOT NULL,
    pipeline_run_id      TEXT NOT NULL,
    processed_at_utc     TIMESTAMPTZ NOT NULL,
    record_hash          CHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS audit.partition_loads (
    partition_load_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id   TEXT NOT NULL,
    order_year        INTEGER NOT NULL,
    order_month       INTEGER NOT NULL,
    rows_read         INTEGER NOT NULL,
    rows_inserted     INTEGER NOT NULL,
    rows_updated      INTEGER NOT NULL,
    rows_unchanged    INTEGER NOT NULL,
    loaded_at_utc     TIMESTAMPTZ NOT NULL
);

-- Used only by the benchmark command (same shape as the curated table)
CREATE TABLE IF NOT EXISTS audit.benchmark_sales_order_lines
    (LIKE curated.sales_order_lines INCLUDING ALL);