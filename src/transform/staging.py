from datetime import datetime, timezone
from pathlib import Path
import json
import pandas as pd
from src.config import SETTINGS


def build_staging(raw_dir: Path, run_id: str):
    """Create cleaned, typed staging datasets.

    Required rules:
    - Deduplicate by business key, keeping greatest updated_at.
    - Parse timestamps as UTC.
    - Normalize emails/cities and flatten product.category.
    - Validate order quantity/status and product price.
    - Add pipeline_run_id and staged_at_utc audit columns.
    - Write invalid records to data/quarantine/ with a reason.

    Return a dict of staging DataFrames and a quarantine DataFrame.
    """
    raw_path = Path(raw_dir)
    
    # 1. Read Raw Files
    customers_df = pd.read_csv(raw_path / 'customers.csv')
    orders_df = pd.read_csv(raw_path / 'orders.csv')
    
    with open(raw_path / 'products.json', 'r', encoding='utf-8') as f:
        products_data = json.load(f)
    products_df = pd.json_normalize(products_data)

    staged_at = datetime.now(timezone.utc)
    quarantine_records = []

    # --- Customers Transformation ---
    if 'updated_at' in customers_df.columns:
        customers_df['updated_at'] = pd.to_datetime(customers_df['updated_at'], utc=True, errors='coerce')
        customers_df = customers_df.sort_values('updated_at').drop_duplicates(subset=['customer_id'], keep='last')
    
    if 'email' in customers_df.columns:
        customers_df['email'] = customers_df['email'].str.strip().str.lower()
    if 'city' in customers_df.columns:
        customers_df['city'] = customers_df['city'].str.strip().str.title()

    customers_df['pipeline_run_id'] = run_id
    customers_df['staged_at_utc'] = staged_at

    # --- Products Transformation ---
    if 'updated_at' in products_df.columns:
        products_df['updated_at'] = pd.to_datetime(products_df['updated_at'], utc=True, errors='coerce')
        products_df = products_df.sort_values('updated_at').drop_duplicates(subset=['product_id'], keep='last')

    # Validate product unit_price
    valid_prices = products_df['unit_price'] > 0
    invalid_products = products_df[~valid_prices].copy()
    if not invalid_products.empty:
        invalid_products['reason'] = 'Invalid or negative product price'
        quarantine_records.append(invalid_products)
        products_df = products_df[valid_prices]

    products_df['pipeline_run_id'] = run_id
    products_df['staged_at_utc'] = staged_at

    # --- Orders Transformation ---
    if 'updated_at' in orders_df.columns:
        orders_df['updated_at'] = pd.to_datetime(orders_df['updated_at'], utc=True, errors='coerce')
        orders_df = orders_df.sort_values('updated_at').drop_duplicates(subset=['order_id'], keep='last')

    allowed_statuses = SETTINGS['quality']['allowed_order_statuses']
    min_qty = SETTINGS['quality']['min_quantity']
    max_qty = SETTINGS['quality']['max_quantity']

    # Validate orders status and quantities
    valid_status = orders_df['status'].isin(allowed_statuses)
    valid_qty = orders_df['quantity'].between(min_qty, max_qty)
    valid_orders = valid_status & valid_qty

    invalid_orders = orders_df[~valid_orders].copy()
    if not invalid_orders.empty:
        invalid_orders['reason'] = 'Invalid order status or quantity out of bounds'
        quarantine_records.append(invalid_orders)
        orders_df = orders_df[valid_orders]

    orders_df['pipeline_run_id'] = run_id
    orders_df['staged_at_utc'] = staged_at

    # Combine quarantine records if any exist
    if quarantine_records:
        quarantine_df = pd.concat(quarantine_records, ignore_index=True)
    else:
        quarantine_df = pd.DataFrame(columns=list(orders_df.columns) + ['reason'])

    staged_dict = {
        'customers': customers_df,
        'products': products_df,
        'orders': orders_df
    }

    return staged_dict, quarantine_df
