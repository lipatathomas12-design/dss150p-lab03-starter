import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Root directory of the project
ROOT_DIR = Path(__file__).resolve().parent.parent

# Fallback to localhost if running locally, default to 'postgres' for docker
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "changeme")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def path_for(layer: str, *subpaths: str) -> Path:
    """Helper to get standardized absolute paths for data layers (raw, staging, curated, etc.)."""
    path = ROOT_DIR / "data" / layer
    if subpaths:
        path = path.joinpath(*subpaths)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path