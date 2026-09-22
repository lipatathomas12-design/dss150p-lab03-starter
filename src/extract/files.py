from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

<<<<<<< HEAD
=======
    TODO:
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
    1. Create data/raw/run_id=<run_id>/.
    2. Copy customers.csv, products.json, and orders.csv from data/source/.
    3. Return the run-specific raw path.
    4. Do not modify source files in place.
    """
<<<<<<< HEAD
    source_dir = path_for('source_dir')
    raw_base = path_for('raw_dir')
    
    run_raw_dir = raw_base / f"run_id={run_id}"
    run_raw_dir.mkdir(parents=True, exist_ok=True)
  
    source_files = ['customers.csv', 'products.json', 'orders.csv']
    
    for filename in source_files:
        src_path = source_dir / filename
        if src_path.exists():
            shutil.copy2(src_path, run_raw_dir / filename)
            
    return run_raw_dir
=======
    raise NotImplementedError('Implement Week 5 raw extraction')
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
