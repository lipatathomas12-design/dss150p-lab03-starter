import pandas as pd
from pathlib import Path


def build_curated(stg_df: pd.DataFrame, run_id: str):
    """Build curated dataset from staging data."""
    if stg_df is None or stg_df.empty:
        curated_df = pd.DataFrame()
        orphans_df = pd.DataFrame()
        return curated_df, orphans_df

    # Copy staging DataFrame for curated processing
    curated_df = stg_df.copy()
    orphans_df = pd.DataFrame()

    return curated_df, orphans_df