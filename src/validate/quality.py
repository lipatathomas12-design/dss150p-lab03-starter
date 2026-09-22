<<<<<<< HEAD
import pandas as pd
from src.load.postgres import get_db_connection


def validate_curated(df: pd.DataFrame) -> list[str]:
=======
def validate_curated(df) -> list[str]:
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
    """Return a list of human-readable validation errors.

    Minimum checks: order_id uniqueness/non-null, quantity range,
    nonnegative amounts, allowed statuses, required audit fields.
    """
<<<<<<< HEAD
    errors = []
    if df.empty:
        return ["DataFrame is empty"]

    #  order_id uniqueness and non-null
    if df['order_id'].isnull().any():
        errors.append("Null values found in order_id")
    if df['order_id'].duplicated().any():
        errors.append("Duplicate order_id found")

    #  quantity range (> 0)
    if (df['quantity'] <= 0).any():
        errors.append("Invalid quantity values found (must be > 0)")

    #  nonnegative amounts
    for col in ['gross_amount', 'discount_amount', 'net_amount']:
        if col in df.columns and (df[col] < 0).any():
            errors.append(f"Negative values found in {col}")

    #  allowed statuses
    if 'status' in df.columns:
        if df['status'].isnull().any():
            errors.append("Null values found in status")

    #  required audit fields
    required_audit = ['pipeline_run_id', 'processed_at_utc', 'record_hash']
    for col in required_audit:
        if col not in df.columns or df[col].isnull().any():
            errors.append(f"Missing or null required audit field: {col}")

    return errors


def run_quality_checks() -> list[str]:
    """Run quality checks on the curated table loaded in PostgreSQL."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql("SELECT * FROM curated.sales_order_lines", conn)
    except Exception as e:
        err_msg = f"Could not connect to database or query table: {e}"
        print(err_msg)
        return [err_msg]
    
    errors = validate_curated(df)
    if errors:
        print(f"Validation failed with {len(errors)} errors:")
        for err in errors:
            print(f" - {err}")
    else:
        print("All quality validation checks passed successfully!")
    return errors
=======
    raise NotImplementedError('Implement data validation')
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
