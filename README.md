# DSS150P Laboratory 3: Productionizing a Modular Data Pipeline

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.

## Main Progression
- **Goal 1:** Reproducible environment, modularization, Git, Docker, and externalized configuration
- **Goal 2:** Raw -> Staging -> Curated transformations; audit logging, error handling, and rerun-safe PostgreSQL loading
- **Goal 3:** CSV, JSON Lines, Parquet, and PostgreSQL comparison; partitioning; selected-partition loading
- **Goal 4:** Apache Airflow orchestration DAG for extract -> transform -> load -> validate

---

## Required Execution Commands

### Goal 1: Environment & Docker Setup
```powershell
cp .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.cli validate-env
docker compose up -d postgres

### Goal 2: Modular ETL Pipeline Execution
PowerShell
python -m src.cli extract
python -m src.cli transform
python -m src.cli load
python -m src.cli validate

### Goal 3: Storage Benchmarking & Partition Loading
PowerShell
python -m src.cli benchmark --repeats 5
python -m src.cli load-partition --year 2026 --month 1
Goal 4: Airflow Workflow Orchestration
PowerShell
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
Airflow Web UI: http://localhost:8080 (Credentials: admin / admin)