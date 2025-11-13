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

from python_code.main.sql_files.snowflake_sql.create_table import create_table


SNOWFLAKE_CONN_ID = 'snowflake-sandiego-db-employees-schema-employees_s3'
TEMP_all_tables_names = [('employees', 'department'), ('employees', 'employee')]


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

    # 1. Create snowflake tables if don't exist using correct column names and data types based on s3 bucket contents
    # check if table exists in snowflake
        # if not exists, then create the table
    with sf_conn.cursor() as cur:

        # Create the table if it doesn't exist
        cur.execute(f"SELECT 'Hello from Python script in main_s3_to_snowflake.py, will extract data from {schema_name}.{table_name}';")
        print(cur.fetchall())
        cur.execute(create_table(
            schema_name=schema_name,
            table_name=table_name,
        ))

        # Copy all new parquet files for the specific table from s3 to relevant table in snowflake


    sf_conn.commit()


    # 2. After table is created, copy only new parquet files into the table (snowflake handles parallel processing and prevents prev file loads) (dont worry about row updates or deletes for now just inserts)
    # can extract the stage path for snowflake sql code using existing ext stage, schema name and table name

    return 'test successful from main_s3_to_snowflake.py'
