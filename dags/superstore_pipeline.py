from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/scripts")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="superstore_pipeline",
    default_args=default_args,
    description="Retail sales ELT pipeline — Bronze to Silver to Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "medallion"],
) as dag:

    def run_bronze():
        from load_raw_to_bronze import load_raw_to_bronze
        load_raw_to_bronze()

    def run_silver():
        from clean_to_silver import clean_bronze_to_silver
        clean_bronze_to_silver()

    extract_load_bronze = PythonOperator(
        task_id="extract_load_to_bronze",
        python_callable=run_bronze,
    )

    clean_load_silver = PythonOperator(
        task_id="clean_load_to_silver",
        python_callable=run_silver,
    )

    dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select gold",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )

    extract_load_bronze >> clean_load_silver >> dbt_gold >> dbt_test