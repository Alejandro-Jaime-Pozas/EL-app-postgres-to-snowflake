# Code from dags/main/main.py
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from python_code.main.main import (
    get_pg_cursor,
    get_schemas,
    extract_pg_table_data_to_s3,
    _s3fs_from_airflow_conn,

)


@dag(
    dag_id='main_dag_v01',
    description='Runs the ETL process to extract from psql db > s3 > snowflake.',
    start_date=datetime(2025, 11, 1),
    schedule=None,
    dagrun_timeout=timedelta(minutes=60),
)
def ETLPostgressToS3ToSnowflake():

    @task
    def check_pg_conn():
        # get_schemas()
        # extract_pg_table_data_to_s3(
        #     'employees',
        #     'employee',
        # )
        _s3fs_from_airflow_conn

    check_pg_conn()

ETLPostgressToS3ToSnowflake()
