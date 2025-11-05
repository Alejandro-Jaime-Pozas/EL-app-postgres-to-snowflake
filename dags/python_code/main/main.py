# TODO need to extract data from all schemas that contain tables from postgres db hosted in neon
    # dag outside this file in dags folder will contain final script, not this file, only for functions
    # use python functions for now, each main fn (ETL) should translate to a task in the final dag (roughly)
    # use airflow hooks/operators instead of directly from postgres/snowflake connectors


# import os
# import pandas as pd
# import pyarrow as pa
# import pyarrow.dataset as ds
# from sqlalchemy import create_engine, text

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Connections Airflow UI
PG_CONN_ID='pg-local'  # not local but ok, change later
SF_CONN_ID='sf-default'

# Extract from postgres


# # Load to S3
# S3_URI = "s3://postgres-neon-db-employees/db-data/schemas/"  # partition path you like
# ROWS_PER_FILE = 250_000


# 1. extract all schemas from the db from information_schema
# prob need a pg cursor i can reuse in other fns
def get_pg_cursor():
    """Connect to psql and return cursor."""
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    # print('cursor successfully extracted: ', cur)
    return cur


def get_sqlalchemy_conn_uri(conn_id: str) -> str:
    """
    Returns a SQLAlchemy-style connection URI from an Airflow connection.
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    uri = conn.get_uri()
    return uri


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
# # pip install psycopg2-binary SQLAlchemy pandas pyarrow s3fs

# engine = create_engine(PG_DSN)

# # Use a consistent snapshot so the whole export sees one point in time
# with engine.begin() as conn:
#     conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
#     # Narrow/select columns explicitly if needed
#     sql = text("SELECT * FROM public.my_table ORDER BY id")
#     chunks = pd.read_sql(sql, conn, chunksize=50_000)

#     # Set up Arrow S3 filesystem sink and write in multiple files
#     fs = pa.fs.S3FileSystem()  # reads AWS creds from env/profile/role

#     # We'll append to a dataset; Arrow rotates files for us.
#     writer = ds.DatasetWriter(
#         base_dir=S3_URI,
#         schema=None,                   # infer from first batch
#         filesystem=fs,
#         format=ds.ParquetFileFormat(), # Parquet output
#         max_rows_per_file=ROWS_PER_FILE
#     )

#     try:
#         for df in chunks:
#             table = pa.Table.from_pandas(df, preserve_index=False)
#             writer.write_table(table)
#     finally:
#         writer.finish()



# 3. for each table, extract column names, data types, all values to be able to accurately map to snowflake



# 4. create snowflake tables if don't exist using correct column names and data types
# 5. insert (or copy) postgres data into its corresponding snowflake table incrementally (or full load if first sync)
