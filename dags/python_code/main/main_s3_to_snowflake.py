# Functionality related to transferring s3 files to snowflake.

# No need for s3 queue check nor pipe since airflow tasks handle execution
# Will use snowflake hook for connection from airflow
# Will use infer schema to auto-assign column names and data types for tables
# Will use snowflake external stage to link s3 bucket to snowflake for instant access/bucket updates
# Will use normal snowflake tables, not external to dump data since normal tables more efficient

# STEPS
    # get list of schemas-tables from s3 upload
    # loop through each schema-table

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from python_code.main.sql_files.snowflake_sql.alter_table_add_metadata import alter_table_add_metadata
from python_code.main.sql_files.snowflake_sql.copy_into_table import copy_into_table
from python_code.main.sql_files.snowflake_sql.create_schema import create_schema
from python_code.main.sql_files.snowflake_sql.create_table import create_table


SNOWFLAKE_CONN_ID = 'snowflake-sandiego-db-employees-schema-employees_s3'


def get_sf_conn(snowflake_conn_id=SNOWFLAKE_CONN_ID):
    """ Get the airflow snowflake connection. """
    hook = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id
    )
    conn = hook.get_conn()
    print('Getting snowflake conn success, connected to acct:', conn.account)
    return conn


def copy_s3_data_into_snowflake(
    sf_conn,  # pass in from main_dag
    schema_name: str = None,
    table_name: str = None,
):
    """
    Extracts data from a single s3 table run path
    and copies into snowflake relevant table.
    """

    # TODO
        # TODO later implement not specifying the db in the airflow connection, instead leave empty and activate the db separately
        # create the database if not exists with same name as postgres database
        # create the schema if not exists with same name as pg schema
        # use the db, use the schema
        # create the snowflake s3 integration (unsure if needed bc of new bucket, check this out)
            # looks like I only need to change IAM s3 permissions to include access to all buckets..snowflake only reqs 1 integration per service
        # create the parquet file format in schema
        # create the stage if not exists using same name as s3 bucket

    table_files_path = f'@POSTGRES_NEON_DB_EMPLOYEES/db-data/schemas/{schema_name}/tables/{table_name}/'  # use for snowflake stage path

    # 1. Create snowflake tables if don't exist using correct column names and data types based on s3 bucket contents

    with sf_conn.cursor() as cur:

        # Schema must exist piror to have access to s3 ext stage parquet files...
        # # Create the schema if it doesn't exist yet
        # print(f'''Creating schema {schema_name} if doesn't exist...''')
        # cur.execute(create_schema(schema_name=schema_name))

        # Create the table if it doesn't exist yet
        print(f'''Creating table {schema_name}.{table_name} if doesn't exist...''')
        try:
            cur.execute(create_table(
                schema_name=schema_name,
                table_name=table_name,
                table_files_path=table_files_path,
            ))
            msg = cur.fetchone()[0]
            print(msg)

            # If table was created, then add the metadata columns
            # TODO if table already exists, but for some reason medatacolumns not added, will fail
            if 'created' in msg and 'exists' not in msg:
                print(f'''Adding table metadata cols...''')
                cur.execute(alter_table_add_metadata(
                    schema_name=schema_name,
                    table_name=table_name,
                ))

        except Exception as e:
            print('Error while executing create table statement:', e)
            raise

        # Copy all new parquet files for the specific table from s3 to relevant table in snowflake
        print(f'''Copying new files from {table_files_path} to table {schema_name}.{table_name}...''')
        cur.execute(copy_into_table(
            schema_name=schema_name,
            table_name=table_name,
            table_files_path=table_files_path,
        ))


    sf_conn.commit()


    # 2. After table is created, copy only new parquet files into the table (snowflake handles parallel processing and prevents prev file loads) (dont worry about row updates or deletes for now just inserts)
    # can extract the stage path for snowflake sql code using existing ext stage, schema name and table name

    return 'test successful from main_s3_to_snowflake.py'
