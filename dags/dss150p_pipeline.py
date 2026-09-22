from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'

def failure_callback(context):
<<<<<<< HEAD
    print(f"TASK FAILED: {context['task_instance'].task_id} in DAG: {context['dag'].dag_id}")
=======
    # TODO Goal 4: write a concise failure record or print meaningful context.
    print('TASK FAILED:', context['task_instance'].task_id)
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3

DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
<<<<<<< HEAD
    tags=['DSS150P', 'Medallion'],
) as dag:

    # Conditional command based on Airflow dag_run params
    load_command = (
        f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
        f'python -m src.cli load-partition --year {{{{ dag_run.conf.get("year", params.year) }}}} --month {{{{ dag_run.conf.get("month", params.month) }}}}'
        if "{{ dag_run.conf.get('run_mode', params.run_mode) }}" == "partition"
        else f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli load'
    )

=======
    tags=['DSS150P'],
) as dag:
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )
<<<<<<< HEAD
    
=======
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )
<<<<<<< HEAD
    
    load = BashOperator(
        task_id='load',
        bash_command=load_command,
    )
    
=======
    load = BashOperator(
        task_id='load',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli load',
    )
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

<<<<<<< HEAD
    extract >> transform >> load >> validate
=======
    # TODO Goal 4: confirm dependencies, timeouts, parameter usage,
    # and a deliberate failure/recovery experiment.
    extract >> transform >> load >> validate
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
