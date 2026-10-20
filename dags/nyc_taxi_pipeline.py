from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project"
DBT_ENV = "export DBT_TARGET_PATH=/tmp/dbt_target DBT_LOG_PATH=/tmp/dbt_logs"


@dag(
    dag_id="nyc_taxi_pipeline",
    schedule=None,
    catchup=False,
    params={"month": "2024-06"},
)
def nyc_taxi_pipeline():
    download = BashOperator(
        task_id="download",
        bash_command="cd /opt/airflow && python scripts/download_taxi.py {{ params.month }}",
    )

    load_raw = BashOperator(
        task_id="spark_load_raw",
        bash_command="cd /opt/airflow && python scripts/spark_load_raw.py {{ params.month }}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{DBT_ENV} && {DBT} run {DBT_ARGS} "
            """--vars "{batch_month: '{{ params.month }}'}" """
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_ENV} && {DBT} test {DBT_ARGS}",
    )

    download >> load_raw >> dbt_run >> dbt_test


nyc_taxi_pipeline()
