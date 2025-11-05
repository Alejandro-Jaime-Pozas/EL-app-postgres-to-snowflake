# TODO need to extract data from all schemas that contain tables from postgres db hosted in neon
    # dag outside this file in dags folder will contain final script, not this file, only for functions
    # use python functions for now, each main fn (ETL) should translate to a task in the final dag (roughly)
    # use airflow hooks/operators instead of directly from postgres/snowflake connectors


import os

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from sqlalchemy import text
from python_code.main.sql_files.get_all_table_data import get_all_table_data

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Connections Airflow UI
PG_CONN_ID='pg-local'  # not local but ok, change later
SF_CONN_ID='sf-default'

# Extract from postgres
CHUNK_SIZE=50_000

# Load to S3
S3_URI="s3://postgres-neon-db-employees/db-data/schemas/"  # then schema name, tables, table name, partition name
ROWS_PER_FILE=250_000


# 1. extract all schemas from the db from information_schema
# need to connect to postgres db

def get_pg_hook(postgres_conn_id: str = PG_CONN_ID):
    """Connect to postgres and return hook."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    print('Getting pg hook success.')
    return hook


# prob need a pg cursor i can reuse in other fns
def get_pg_cursor():
    """Connect to psql and return cursor."""
    hook = get_pg_hook()
    conn = hook.get_conn()
    cur = conn.cursor()
    print('cursor successfully extracted: ', cur)
    return cur


def get_sqlalchemy_engine():
    """Returns a SQLAlchemy engine to use."""
    pg_hook = get_pg_hook()
    pg_engine = pg_hook.get_sqlalchemy_engine()
    print('Getting sqlalchemy pg engine success.')
    return pg_engine


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
    print('schema and table names successfully extracted', schema_names)
    return schema_names


# 2.5 copy all psql tables into s3 bucket as parquet files, use <proj_name>/db-data/schemas/<schema>/<table> for bucket location
# pip install psycopg2-binary SQLAlchemy pandas pyarrow s3fs

def extract_pg_table_data(
    schema_name: str,
    table_name: str,
    sql: str = 'select 1;',
    chunksize: int = CHUNK_SIZE,
):
    """Extracts data from a single pg table."""

    pg_engine = get_sqlalchemy_engine()

    try:
        with pg_engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            for df in pd.read_sql(f'select 1;', conn, chunksize=chunksize):
                print(df.__dict__)
    finally:
        pass

# 3. for each table, extract column names, data types, all values to be able to accurately map to snowflake?



# 4. create snowflake tables if don't exist using correct column names and data types


# 5. overwrite (later insert) postgres data into its corresponding snowflake table incrementally (or full load if first sync)
