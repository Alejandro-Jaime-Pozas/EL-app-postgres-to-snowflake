# dag outside this file in dags folder will contain final script, not this file, only for functions
# use airflow hooks/operators instead of directly from postgres/snowflake connectors if possible


from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from python_code.main.sql_files.pg_sql.get_all_schemas_and_tables import get_all_schemas_and_tables
from python_code.main.sql_files.pg_sql.get_all_table_data import get_all_table_data

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


# Connections Airflow UI
# PG_CONN_ID='pg-local'  # not local, connects to neon db but ok, change later
PG_CONN_ID='postgres_stock_app'  # stock application db in neon
AWS_CONN_ID='aws-sandiego'

DB_NAME = 'stock_app'

# Extract from postgres
CHUNK_SIZE=50_000

# Load to S3
S3_URI=f"postgres-neon-db-{DB_NAME.replace('_','-')}/db-data/schemas"  # then schema name, tables, table name, run timestamp, files
ROWS_PER_FILE=250_000


# 1. Get pg hook and connection

# Need to connect to postgres db
def get_pg_hook(postgres_conn_id: str = PG_CONN_ID):
    """Connect to postgres and return hook."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    print('Getting pg hook success.')
    return hook


# Prob need a pg conn i can reuse in other fns
def get_pg_conn(pg_hook: PostgresHook = get_pg_hook()):
    """Connect to psql and return connection."""
    conn = pg_hook.get_conn()
    print('Postgres connection object successfully extracted: ', conn)
    return conn

# Get airflow pg conn object info
def get_pg_conn_info(
    pg_hook: PostgresHook = get_pg_hook(),
    postgres_conn_id: str = PG_CONN_ID
):
    """Get the connection details to extract database name."""
    conn_info = pg_hook.get_connection(postgres_conn_id)
    conn_db_name = conn_info.schema
    print('The pg conn database name is:', conn_db_name)
    return conn_info

# 2. For each schema, extract all table names for that schema using information_schema

def get_schemas(pg_conn):
    """Get all schemas from postgres db."""
    # conn = get_pg_conn()  # now called in main_dag
    cur = pg_conn.cursor()
    sql = get_all_schemas_and_tables()

    cur.execute(sql)
    schema_table_names = cur.fetchall()

    exclude_tables = [
        ('employees', 'salary'),
        ('employees', 'department_employee'),
        ('employees', 'title'),
    ]  # exclude some large tables to prevent usage limits s3/snowflake
    schema_table_names = [t for t in schema_table_names if t not in exclude_tables]

    print('Schema and table names successfully extracted:', schema_table_names)
    return schema_table_names


# Retrieve airflow aws s3 conn
def _s3fs_from_airflow_conn(aws_conn_id: str = AWS_CONN_ID, region_name: str | None = None):
    """ Return the s3 file system object. """
    aws = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="sts")
    creds = aws.get_credentials()
    s3_filesystem = pafs.S3FileSystem(
        access_key=creds.access_key,
        secret_key=creds.secret_key,
        session_token=creds.token,
        region=region_name,
        allow_bucket_creation=True,
    )
    print('Success retrieving aws creds and s3 filesystem:', s3_filesystem)
    return s3_filesystem


# Use pyarrow dataset write fn to write files to s3 storage location
def write_parquet_to_s3(
    table_data,
    s3_uri: str,
    filesystem: pafs.S3FileSystem,
    max_rows_per_file: int = ROWS_PER_FILE,
):
    """ Use pyarrow dataset ds to write data into parquet files in s3. """
    # print('Writer ready for s3...')
    ds.write_dataset(
        data=table_data,
        base_dir=s3_uri,
        schema=None,
        filesystem=filesystem,
        format='parquet',  # default compression is uncompressed, snowflake can handle
        max_rows_per_file=max_rows_per_file,
        max_rows_per_group=max_rows_per_file,
        existing_data_behavior='overwrite_or_ignore',
    )
    return 0


# 2.5 Copy all psql tables into s3 bucket as parquet files,
    # use <proj_name>/db-data/schemas/<schema>/tables/<table>/run_<timestamp>/ for write file location
# pip install psycopg2-binary SQLAlchemy pandas pyarrow s3fs

# 2.5.1 Extract all data from a single pg table
def extract_pg_table_data_to_s3(
    schema_name: str,
    table_name: str,
    pg_conn,
    s3_filesystem_conn,
    s3_uri: str = S3_URI,
    chunksize: int = CHUNK_SIZE,
    max_rows_per_file: int = ROWS_PER_FILE,
):
    """Extracts data from a single pg table."""

    sql = get_all_table_data(schema_name, table_name)

    # Set transaction isolation level if needed
    with pg_conn.cursor() as cur:
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    pg_conn.commit()

    # Create the detailed s3 uri for the file
    # Each table should have a subfolder with the current run timestamp, and within this folder, multiple parquet files depending on table size
    s3_file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_uri_detail = f'{s3_uri}/{schema_name}/tables/{table_name}/run_{s3_file_timestamp}/'
    print('S3 filepath to use:', s3_uri_detail)

    # Accumulate all chunks into a list
    # TODO this approach stores in memory the entire table being read from pg...not ideal if large table need to change
    all_tables = []
    current_chunk = 0

    # Pass the raw psycopg2 connection directly - pandas will use it for chunked reading
    for df in pd.read_sql(sql, pg_conn, chunksize=chunksize):

        print('READING CHUNK FROM ROW:', current_chunk, 'TO', current_chunk + chunksize)
        current_chunk += chunksize

        # Add parquet-ready pa table to all tables
        tbl = pa.Table.from_pandas(df, preserve_index=False)
        all_tables.append(tbl)

    # Concatenate all chunks into one big table
    full_table = pa.concat_tables(all_tables)
    print(f'Total rows collected: {len(full_table)}')

    # Write once - PyArrow will split into multiple files based on max_rows_per_file
    print(f'Begin writing table {schema_name}.{table_name} to s3...')
    writer = write_parquet_to_s3(
        table_data=full_table,
        filesystem=s3_filesystem_conn,
        s3_uri=s3_uri_detail,
        max_rows_per_file=max_rows_per_file,
    )

    if writer == 0:
        return 0
    else:
        return 1
