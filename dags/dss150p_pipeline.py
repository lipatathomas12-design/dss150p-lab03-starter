from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("PIPELINE_PROJECT_DIR", "/opt/airflow/project")
CLI = f"cd {PROJECT_DIR} && python -m src.cli"
RID = "--run-id '{{ run_id }}'"  # one Airflow run id shared by ALL tasks -> consistent pipeline_run_id
PART = "--year {{ params.year }} --month {{ params.month }}"


def on_failure(context):
    ti = context["task_instance"]
    print(f"[FAILURE CALLBACK] dag={ti.dag_id} task={ti.task_id} run_id={context['run_id']} "
          f"try={ti.try_number} error={context.get('exception')} log={ti.log_url}")


def on_retry(context):
    ti = context["task_instance"]
    print(f"[RETRY CALLBACK] task={ti.task_id} run_id={context['run_id']} try={ti.try_number} "
          f"error={context.get('exception')}")


default_args = {
    "owner": "dss150p",
    "retries": 2,                                  # >= 2 retries
    "retry_delay": timedelta(minutes=1),           # explicit delay
    "execution_timeout": timedelta(minutes=15),    # a task can never hang forever
    "on_failure_callback": on_failure,
    "on_retry_callback": on_retry,
}

with DAG(
    dag_id="dss150p_pipeline",
    description="extract -> transform -> load -> validate",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",      # daily 02:00: after the source systems' end-of-day export, before analysts start
    catchup=False,             # no automatic historical runs; backfills are explicit (idempotent load makes them safe)
    max_active_runs=1,
    default_args=default_args,
    params={
        "run_mode": Param("full", type="string", enum=["full", "partition"]),
        "year": Param(2026, type="integer", minimum=2000, maximum=2100),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
    tags=["dss150p", "lab3"],
) as dag:
    extract = BashOperator(task_id="extract", bash_command=f"{CLI} extract {RID}")
    transform = BashOperator(task_id="transform", bash_command=f"{CLI} transform {RID}")
    load = BashOperator(
        task_id="load",
        bash_command=(f"{CLI} {{% if params.run_mode == 'partition' %}}load-partition {RID} {PART}"
                      f"{{% else %}}load {RID}{{% endif %}}"),
    )
    validate = BashOperator(
        task_id="validate",
        bash_command=(f"{CLI} validate {RID}{{% if params.run_mode == 'partition' %}} {PART}{{% endif %}}"),
    )
    extract >> transform >> load >> validate