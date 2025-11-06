# TODO need to extract data from all schemas that contain tables from postgres db hosted in neon
    # dag outside this file in dags folder will contain final script, not this file, only for functions
    # use python functions for now, each main fn (ETL) should translate to a task in the final dag (roughly)
    # use airflow hooks/operators instead of directly from postgres/snowflake connectors


import os
from re import S

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from sqlalchemy import text
from python_code.main.sql_files.get_all_table_data import get_all_table_data

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Connections Airflow UI
PG_CONN_ID='pg-local'  # not local but ok, change later
SF_CONN_ID='sf-default'
AWS_CONN_ID='aws-sandiego'

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
def get_pg_conn():
    """Connect to psql and return cursor."""
    hook = get_pg_hook()
    conn = hook.get_conn()
    print('pg connection obj successfully extracted: ', conn)
    return conn


# 2. for each schema, extract all table names for that schema using information_schema
def get_schemas():
    """Get all schemas from postgres db."""
    conn = get_pg_conn()
    cur = conn.cursor()
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

# 2.5.1 extract all data from a single pg table
def extract_pg_table_data_to_s3(
    schema_name: str,
    table_name: str,
    chunksize: int = CHUNK_SIZE,
    s3_uri: str = S3_URI,
    max_rows_per_file: int = ROWS_PER_FILE,
):
    """Extracts data from a single pg table."""

    sql = get_all_table_data(schema_name, table_name)

    # Use PostgresHook to get a raw psycopg2 connection
    # pandas with chunksize works best with raw DBAPI connections
    pg_conn = get_pg_conn()

    try:
        # Set transaction isolation level if needed
        with pg_conn.cursor() as cur:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        pg_conn.commit()

        current_chunk = 0
        # Pass the raw psycopg2 connection directly - pandas will use it for chunked reading
        for df in pd.read_sql(sql, pg_conn, chunksize=chunksize):

            # print(df.head())  # best for quick look at all data
            print('STARTING ITER FROM ROW:', current_chunk, 'TO', current_chunk + chunksize)
            current_chunk += chunksize

            # create table format ready to load to parquet file format
            tbl = pa.Table.from_pandas(df, preserve_index=False)

            # # export table to parquet file in s3
            # s3_filesystem_conn = _s3fs_from_airflow_conn()
            # writer = parquet_data_writer_obj(
            #     filesystem=s3_filesystem_conn,
            #     s3_uri=s3_uri_detail,
            #     max_rows_per_file=max_rows_per_file,
            # )

    finally:
        pg_conn.close()


# retrieve airflow aws s3 conn
def _s3fs_from_airflow_conn(aws_conn_id: str = AWS_CONN_ID, region_name: str | None = None):
    aws = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="sts")
    creds = aws.get_credentials()
    s3_filesystem = pafs.S3FileSystem(
        access_key=creds.access_key,
        secret_key=creds.secret_key,
        session_token=creds.token,
        region=region_name,
    )
    print('Success retrieving aws creds and s3 filesystem', s3_filesystem)
    return s3_filesystem


# use pyarrow dataset write fn to write files to s3 storage location
def parquet_data_writer_obj(
    filesystem: pafs.S3FileSystem,
    s3_uri: str = S3_URI,
    max_rows_per_file: int = ROWS_PER_FILE
):
    writer = ds.DatasetWriter(
        base_dir=s3_uri,  # fix!!!
        schema=None,
        filesystem=filesystem,
        format=ds.ParquetFileFormat(),  # default compression is uncompressed, snowflake can handle
        max_rows_per_file=max_rows_per_file
    )
    print('Writer ready for s3...')
    return writer


# 3. for each table, extract column names, data types, all values to be able to accurately map to snowflake?



# 4. create snowflake tables if don't exist using correct column names and data types


# 5. overwrite (later insert) postgres data into its corresponding snowflake table incrementally (or full load if first sync)
