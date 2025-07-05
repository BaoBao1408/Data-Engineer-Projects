from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from scripts.load_delta_data import get_last_loaded_timestamp, load_data_to_staging

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': True,
    'retries': 1,
}

# Directory for delta data
DELTA_DIR = '/opt/airflow/data/delta'

# Define the DAG
with DAG(
    'etl_pipeline_optimized',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    # Task: Incremental load with timestamp check
    incremental_load_task = PythonOperator(
        task_id='incremental_load_to_staging',
        python_callable=load_data_to_staging,
        op_kwargs={'path_dirr': DELTA_DIR},
    )

    # Task: Run dbt transformations
    dbt_run = BashOperator(
        task_id='run_dbt',
        bash_command='dbt run --project-dir /opt/dbt',
    )

    # Define task dependencies
    incremental_load_task >> dbt_run
