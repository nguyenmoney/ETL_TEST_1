FROM apache/airflow:latest-python3.12

RUN pip install --no-cache-dir requests pyarrow "psycopg[binary]"

RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres