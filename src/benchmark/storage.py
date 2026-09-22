<<<<<<< HEAD
import os
import time
import statistics
import pandas as pd
from pathlib import Path
from src.load.postgres import get_db_connection


def write_partitioned_parquet(df: pd.DataFrame, output_dir: str):
    """Write Parquet partitioned by order_year/order_month."""
    out_path = Path(output_dir) / "partitioned_parquet"
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Ensure order_year and order_month exist
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['order_year'] = df['order_date'].dt.year
        df['order_month'] = df['order_date'].dt.month

    # Write using pandas to_parquet partition_cols if supported, or manually group
    if {'order_year', 'order_month'}.issubset(df.columns):
        df.to_parquet(out_path, partition_cols=['order_year', 'order_month'], index=False)
    else:
        df.to_parquet(out_path / "data.parquet", index=False)


def run_benchmark(curated_path: str = None, output_dir: str = "data/benchmark", repeats: int = 5):
=======
def run_benchmark(curated_path, output_dir, repeats: int = 5):
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL.

    Capture:
    - storage/file size where applicable
    - write time
    - full-read time
    - filtered-read/query time
    - row count

    Use multiple repetitions and report a median for read/query timing.
    """
<<<<<<< HEAD
    os.makedirs(output_dir, str(True) if isinstance(output_dir, str) else None)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    #  Fetch dataframe from postgres or curated file
    try:
        with get_db_connection() as conn:
            df = pd.read_sql("SELECT * FROM curated.sales_order_lines", conn)
    except Exception:
        if curated_path and os.path.exists(curated_path):
            df = pd.read_parquet(curated_path)
        else:
            raise RuntimeError("No database connection or valid curated file found for benchmarking.")

    row_count = len(df)
    results = []

    # File paths
    csv_path = out_dir / "dataset.csv"
    json_path = out_dir / "dataset.jsonl"
    parquet_path = out_dir / "dataset.parquet"

    # --- CSV Benchmark ---
    start = time.time()
    df.to_csv(csv_path, index=False)
    csv_write_time = time.time() - start
    csv_size = csv_path.stat().st_size

    csv_reads = []
    for _ in range(repeats):
        start = time.time()
        _ = pd.read_csv(csv_path)
        csv_reads.append(time.time() - start)
    
    csv_filtered_reads = []
    for _ in range(repeats):
        start = time.time()
        _ = pd.read_csv(csv_path, nrows=1000)
        csv_filtered_reads.append(time.time() - start)

    results.append({
        "format": "CSV",
        "size_bytes": csv_size,
        "write_time_sec": csv_write_time,
        "full_read_median_sec": statistics.median(csv_reads),
        "filtered_read_median_sec": statistics.median(csv_filtered_reads),
        "row_count": row_count
    })

    # --- JSON Lines Benchmark ---
    start = time.time()
    df.to_json(json_path, orient="records", lines=True)
    json_write_time = time.time() - start
    json_size = json_path.stat().st_size

    json_reads = []
    for _ in range(repeats):
        start = time.time()
        _ = pd.read_json(json_path, orient="records", lines=True)
        json_reads.append(time.time() - start)

    json_filtered_reads = []
    for _ in range(repeats):
        start = time.time()
        _ = pd.read_json(json_path, orient="records", lines=True, nrows=1000)
        json_filtered_reads.append(time.time() - start)

    results.append({
        "format": "JSON Lines",
        "size_bytes": json_size,
        "write_time_sec": json_write_time,
        "full_read_median_sec": statistics.median(json_reads),
        "filtered_read_median_sec": statistics.median(json_filtered_reads),
        "row_count": row_count
    })

    # --- Parquet Benchmark ---
    start = time.time()
    df.to_parquet(parquet_path, index=False)
    parquet_write_time = time.time() - start
    parquet_size = parquet_path.stat().st_size

    parquet_reads = []
    for _ in range(repeats):
        start = time.time()
        _ = pd.read_parquet(parquet_path)
        parquet_reads.append(time.time() - start)

    parquet_filtered_reads = []
    for _ in range(repeats):
        start = time.time()
        _ = pd.read_parquet(parquet_path, columns=["order_id", "net_amount"])
        parquet_filtered_reads.append(time.time() - start)

    results.append({
        "format": "Parquet",
        "size_bytes": parquet_size,
        "write_time_sec": parquet_write_time,
        "full_read_median_sec": statistics.median(parquet_reads),
        "filtered_read_median_sec": statistics.median(parquet_filtered_reads),
        "row_count": row_count
    })

    # --- PostgreSQL Benchmark ---
    pg_reads = []
    with get_db_connection() as conn:
        for _ in range(repeats):
            start = time.time()
            pd.read_sql("SELECT * FROM curated.sales_order_lines", conn)
            pg_reads.append(time.time() - start)

    pg_filtered_reads = []
    with get_db_connection() as conn:
        for _ in range(repeats):
            start = time.time()
            pd.read_sql("SELECT order_id, net_amount FROM curated.sales_order_lines LIMIT 1000", conn)
            pg_filtered_reads.append(time.time() - start)

    results.append({
        "format": "PostgreSQL",
        "size_bytes": 0,  # External database storage
        "write_time_sec": 0.0,
        "full_read_median_sec": statistics.median(pg_reads),
        "filtered_read_median_sec": statistics.median(pg_filtered_reads),
        "row_count": row_count
    })

    # Print results summary table
    res_df = pd.DataFrame(results)
    print("\n=== STORAGE & PERFORMANCE BENCHMARK RESULTS ===")
    print(res_df.to_string(index=False))
    
    # Save benchmark results
    res_csv_path = out_dir / "benchmark_results.csv"
    res_df.to_csv(res_csv_path, index=False)
    print(f"\nBenchmark results saved to {res_csv_path}")
    return res_df


def run_benchmarks(repeats: int = 5):
    """Wrapper expected by CLI to execute storage benchmarks."""
    return run_benchmark(repeats=repeats)
=======
    raise NotImplementedError('Implement Week 6 storage benchmark')


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    raise NotImplementedError('Implement Week 6 partitioning')
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
