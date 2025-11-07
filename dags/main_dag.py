# Code from dags/main/main.py
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from python_code.main.main import (
    get_pg_conn,
    get_schemas,
    extract_pg_table_data_to_s3,
    _s3fs_from_airflow_conn,

)


@dag(
    dag_id='main_dag_v02',
    description='Runs the ETL process to extract from psql db > s3 > snowflake.',
    start_date=datetime(2025, 11, 1),
    schedule=None,
    dagrun_timeout=timedelta(minutes=60),
)
def ETLPostgressToS3ToSnowflake():

    @task
    def extract_from_postgres_and_upload_to_s3():

        pg_conn = get_pg_conn()

        all_table_names = get_schemas(pg_conn)

        for schema_name, table_name in all_table_names:

            print(f'Will begin processing table: {schema_name}.{table_name} ... ')
            table_upload = extract_pg_table_data_to_s3(
                schema_name=schema_name,
                table_name=table_name,
                pg_conn=pg_conn,
            )
            if table_upload == 0:
                print(f'Successfully loaded {schema_name}.{table_name} to s3...')
            else:
                # TODO if there is an error handle the error
                print(f'Error in table upload for {schema_name}.{table_name} ... ')
                return 'Error, check logs'  # include error msg later

        return 0

    extract_from_postgres_and_upload_to_s3() # >> check_or_create_snowflake_integration >>

ETLPostgressToS3ToSnowflake()
