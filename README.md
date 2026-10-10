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