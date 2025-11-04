# Code from dags/main/main.py
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from python_code.main.main import (
    get_pg_cursor,
    get_schemas,

)


@dag(
    dag_id='main_dag_v01',
    description='Runs the ETL process to extract from psql db > Snowflake.',
    start_date=datetime(2025, 11, 1),
    schedule=None,
    dagrun_timeout=timedelta(minutes=60),
)
def ETLPostgressToSnowflake():

    @task
    def check_pg_conn():
        get_schemas()

    check_pg_conn()

ETLPostgressToSnowflake()
