from datetime import datetime, timedelta

from python_code.spark_main_ai_airflow_safe import main as extract_and_load

from airflow.sdk import dag, task


default_args = {
    'owner': 'Alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


# just need a simple dag schedule to trigger the python code...which is an overwrite of the table for now (later implement insert on date cursor)
@dag(
    dag_id='spark_postgres_employee_schema_to_snowflake_v01',
    start_date=datetime(2025, 10, 26),
    schedule=None  # tweak schedule for testing
)
def overwrite_tables_to_snowflake():

    @task()
    def extract_and_load_task():
        return extract_and_load()

    extract_and_load_task()


trigger_overwrite_tables_to_snowflake = overwrite_tables_to_snowflake()
