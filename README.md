# DSS150P Laboratory 3: Productionizing a Modular Data Pipeline

raw -> staging -> curated (+ quarantine) -> PostgreSQL, with benchmarks, partitioning and Airflow orchestration.

## Setup (host, Windows PowerShell)
```powershell
cp .env.example .env          # then set your own POSTGRES_PASSWORD (never commit .env)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m src.cli validate-env
docker compose up -d postgres
```
Host commands use `POSTGRES_HOST=localhost`. Inside Docker, compose overrides it to `postgres`.

## Run the pipeline
```powershell
python -m src.cli run-all                 # extract + transform; prints run_id, counts, quarantine reasons
python -m src.cli load
python -m src.cli load                    # rerun: unchanged == total, no duplicates
python -m src.cli validate
python -m src.cli benchmark --repeats 5   # writes data/benchmarks/benchmark_results.csv
python -m src.cli load-partition --year 2026 --month 1
python -m pytest
```
Rerun-safe check:
```powershell
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders FROM curated.sales_order_lines;"
```

## Airflow
```powershell
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
UI: http://localhost:8080 (training credentials admin/admin, local lab only).
DAG `dss150p_pipeline`: extract -> transform -> load -> validate. Params: `run_mode` (full|partition), `year`, `month`.

## Layout
- `src/extract` raw snapshot copy, `src/transform` staging/curated/quarantine, `src/load` UPSERT and partitions,
  `src/validate` data contracts, `src/benchmark` format timing, `src/cli.py` thin entry points.
- `dags/dss150p_pipeline.py` calls the CLI only. No transformation logic lives in Airflow.
- `config/settings.yml` non-secret settings; `.env` secrets (untracked); `src/config.py` is the only translator.

## Design notes
- Quarantine holds data-quality rejects with a reason; thrown `PipelineStageError` is for system failures.
- `record_hash` covers business content only (never run id or processing time).
- UPSERT updates a row only when `record_hash` differs.
- Evidence is in `docs/evidence/`; written analysis is in `reflection.md` and `docs/`.

## AI use acknowledgment
Generative AI was used as a coding and writing aid for this lab, within the course policy. I reviewed and can explain every submitted part.