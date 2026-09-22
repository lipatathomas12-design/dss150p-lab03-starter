from contextlib import contextmanager
import psycopg
import pandas as pd
from src.config import DB


def ensure_database_exists():
    """Ensure the target database exists by connecting to the default postgres database."""
    try:
        conn = psycopg.connect(
            host=DB['host'],
            port=DB['port'],
            dbname='postgres',
            user=DB['user'],
            password=DB['password'],
            autocommit=True
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB['dbname'],))
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(f"CREATE DATABASE {DB['dbname']}")
        conn.close()
    except Exception:
        pass


@contextmanager
def get_db_connection():
    """Context manager for PostgreSQL database connections using psycopg v3."""
    ensure_database_exists()
    conn = psycopg.connect(
        host=DB['host'],
        port=DB['port'],
        dbname=DB['dbname'],
        user=DB['user'],
        password=DB['password']
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    Requirement: order_id is the conflict key. A rerun with unchanged records
    must not create duplicate business keys.
    """
    if df.empty:
        return 0

    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS curated;
    CREATE TABLE IF NOT EXISTS curated.sales_order_lines (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50),
        product_id VARCHAR(50),
        quantity INT,
        status VARCHAR(50),
        updated_at TIMESTAMPTZ,
        gross_amount NUMERIC(12, 2),
        discount_amount NUMERIC(12, 2),
        net_amount NUMERIC(12, 2),
        pipeline_run_id VARCHAR(100),
        processed_at_utc TIMESTAMPTZ,
        record_hash VARCHAR(64)
    );
    """

    columns = [
        'order_id', 'customer_id', 'product_id', 'quantity', 'status',
        'updated_at', 'gross_amount', 'discount_amount', 'net_amount',
        'pipeline_run_id', 'processed_at_utc', 'record_hash'
    ]

    insert_df = df[columns].copy()
    
    data_tuples = [
        tuple(None if pd.isna(val) else val for val in row)
        for row in insert_df.to_numpy()
    ]

    upsert_query = """
    INSERT INTO curated.sales_order_lines (
        order_id, customer_id, product_id, quantity, status,
        updated_at, gross_amount, discount_amount, net_amount,
        pipeline_run_id, processed_at_utc, record_hash
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        product_id = EXCLUDED.product_id,
        quantity = EXCLUDED.quantity,
        status = EXCLUDED.status,
        updated_at = EXCLUDED.updated_at,
        gross_amount = EXCLUDED.gross_amount,
        discount_amount = EXCLUDED.discount_amount,
        net_amount = EXCLUDED.net_amount,
        pipeline_run_id = EXCLUDED.pipeline_run_id,
        processed_at_utc = EXCLUDED.processed_at_utc,
        record_hash = EXCLUDED.record_hash;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            cursor.executemany(upsert_query, data_tuples)

    return len(data_tuples)


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    if df.empty:
        return 0

    df['temp_year'] = pd.to_datetime(df['updated_at']).dt.year
    df['temp_month'] = pd.to_datetime(df['updated_at']).dt.month
    
    partition_df = df[(df['temp_year'] == year) & (df['temp_month'] == month)].copy()
    partition_df = partition_df.drop(columns=['temp_year', 'temp_month'])

    if partition_df.empty:
        return 0

    rows_loaded = upsert_curated(partition_df, run_id)

    audit_query = """
    CREATE SCHEMA IF NOT EXISTS audit;
    CREATE TABLE IF NOT EXISTS audit.partition_loads (
        id SERIAL PRIMARY KEY,
        pipeline_run_id VARCHAR(100),
        partition_year INT,
        partition_month INT,
        rows_loaded INT,
        loaded_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO audit.partition_loads (pipeline_run_id, partition_year, partition_month, rows_loaded)
    VALUES (%s, %s, %s, %s);
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(audit_query, (run_id, year, month, rows_loaded))

    return rows_loaded
