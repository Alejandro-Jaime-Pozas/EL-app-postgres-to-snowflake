# TODO need to extract data from all schemas that contain tables from postgres db hosted in neon
    # dag outside this file in dags folder will contain final script, not this file, only for functions
    # use python functions for now, each main fn (ETL) should translate to a task in the final dag (roughly)
    # use airflow hooks/operators instead of directly from postgres/snowflake connectors


from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


PG_CONN_ID='pg-local'  # not local but ok, change later
SF_CONN_ID='sf-default'

# 1. extract all schemas from the db from information_schema
# prob need a pg cursor i can reuse in other fns
def get_pg_cursor():
    """Connect to psql and return cursor."""
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    # print('cursor successfully extracted: ', cur)
    return cur


# 2. for each schema, extract all table names for that schema using information_schema
def get_schemas():
    """Get all schemas from postgres db."""
    cur = get_pg_cursor()
    sql = f"""
        SELECT
            DISTINCT
            table_schema,
            table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
            AND table_schema not in (
                'information_schema',
                'pg_catalog'
            )
        ;
    """

    cur.execute(sql)
    schema_names = cur.fetchall()
    print(schema_names)
    return schema_names


# 2.5 copy all psql tables into s3 bucket as parquet files, use <proj_name>/db-data/schemas/<schema>/<table> for bucket location



# 3. for each table, extract column names, data types, all values to be able to accurately map to snowflake



# 4. create snowflake tables if don't exist using correct column names and data types
# 5. insert (or copy) postgres data into its corresponding snowflake table incrementally (or full load if first sync)
