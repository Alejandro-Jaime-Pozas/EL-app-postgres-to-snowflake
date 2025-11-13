# Code from dags/main/main.py
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from python_code.main.main_s3_to_snowflake import (
    copy_s3_data_into_snowflake,
    get_sf_conn,

)
from python_code.main.main_pg_to_s3 import (
    get_pg_conn,
    get_schemas,
    extract_pg_table_data_to_s3,
    _s3fs_from_airflow_conn,

)


@dag(
    dag_id='main_dag_v03',
    description='Runs the ETL process to extract from psql db > s3 > snowflake.',
    start_date=datetime(2025, 11, 1),
    schedule=None,
    dagrun_timeout=timedelta(minutes=60),
)
def ETLPostgressToS3ToSnowflake():
    """ Dag that extracts tables from postgres db, uploads to s3, then uploads to snowflake. """

    @task
    def extract_from_postgres_and_upload_to_s3():
        """ Extract data from all tables in postgres db and upload data to s3. """

        # Connect to postgres
        pg_conn = get_pg_conn()

        # TODO check if table update is even needed or if there's no new data, don't update..this could even be a separate task?
        # check_if_source_updated = check_if_source_updated()

        # Connect to s3 filesystem
        s3_filesystem_conn = _s3fs_from_airflow_conn()

        # Grab all table names to upload to s3
        all_table_names = get_schemas(pg_conn)

        # # TODO will need to remove for loop and change task to allow for parallel reads/writes of data per table
        # for schema_name, table_name in all_table_names:

        #     print(f'Will begin processing table: {schema_name}.{table_name} ... ')
        #     table_upload = extract_pg_table_data_to_s3(
        #         schema_name=schema_name,
        #         table_name=table_name,
        #         pg_conn=pg_conn,
        #         s3_filesystem_conn=s3_filesystem_conn,
        #     )
        #     if table_upload == 0:
        #         print(f'✅ Successfully loaded {schema_name}.{table_name} to s3...')
        #     else:
        #         # TODO improve error handling
        #         print(f'Error in table upload for {schema_name}.{table_name} ... ')
        #         raise ValueError(f'Error, update failed for {schema_name}.{table_name}. Check logs.')

        # print('✅ Success extracting all tables and loading into s3!')
        return all_table_names  # can return something to pass into the next task so snowflake has access

    @task
    def copy_from_s3_and_upload_to_snowflake(all_table_names: list = None):
        """ Extract table data from s3 and upload into snowflake. """
        # TODO include for loop here to iter through all tables to create/update

        # Connect to Snowflake
        sf_conn = get_sf_conn()

        copy_s3_data_into_snowflake(
            sf_conn=sf_conn,
            schema_name='public',
            table_name='car_sales',
        )

    tables_to_load = extract_from_postgres_and_upload_to_s3()
    snowflake_upload = copy_from_s3_and_upload_to_snowflake(tables_to_load)

ETLPostgressToS3ToSnowflake()
