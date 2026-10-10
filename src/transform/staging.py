import pandas as pd


def build_staging(raw_data, run_id: str):
    """Build staging dataset from raw extracted source files."""
    stg_df = pd.DataFrame()
    quarantine_df = pd.DataFrame()
    return stg_df, quarantine_df