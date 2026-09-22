<<<<<<< HEAD
from datetime import datetime, timezone
import hashlib
import pandas as pd


def compute_record_hash(row: pd.Series) -> str:
    """Compute a deterministic SHA-256 hash for a row based on core business values."""
    key_string = f"{row.get('order_id')}-{row.get('customer_id')}-{row.get('product_id')}-{row.get('quantity')}"
    return hashlib.sha256(key_string.encode('utf-8')).hexdigest()


=======
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows.

    Required columns include gross_amount, discount_amount, net_amount,
    processed_at_utc, pipeline_run_id, and record_hash.

    Orphan customer/product references must be quarantined, not silently dropped.
    """
<<<<<<< HEAD
    orders_df = staging['orders'].copy()
    customers_df = staging['customers'].copy()
    products_df = staging['products'].copy()

    processed_at = datetime.now(timezone.utc)

    # Check for orphan references against customers and products
    valid_customers = orders_df['customer_id'].isin(customers_df['customer_id'])
    valid_products = orders_df['product_id'].isin(products_df['product_id'])
    valid_refs = valid_customers & valid_products

    orphans_df = orders_df[~valid_refs].copy()
    if not orphans_df.empty:
        orphans_df['reason'] = 'Orphan reference: missing customer_id or product_id'

    # Filter orders to only valid foreign keys
    clean_orders = orders_df[valid_refs].copy()

    #  Merge orders with customers and products
    merged_df = clean_orders.merge(customers_df, on='customer_id', suffixes=('', '_cust'), how='inner')
    merged_df = merged_df.merge(products_df, on='product_id', suffixes=('', '_prod'), how='inner')

    # Calculate financial metrics
    # Assuming columns: quantity, unit_price, discount (if available, else 0)
    if 'discount' not in merged_df.columns:
        merged_df['discount'] = 0.0

    merged_df['gross_amount'] = merged_df['quantity'] * merged_df['unit_price']
    merged_df['discount_amount'] = merged_df['gross_amount'] * (merged_df['discount'] / 100.0)
    merged_df['net_amount'] = merged_df['gross_amount'] - merged_df['discount_amount']

    #  Add audit columns
    merged_df['pipeline_run_id'] = run_id
    merged_df['processed_at_utc'] = processed_at

    # Compute record hash for idempotency/integrity tracking
    merged_df['record_hash'] = merged_df.apply(compute_record_hash, axis=1)

    return merged_df, orphans_df
=======
    raise NotImplementedError('Implement Goal 2 curated transformation')
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
