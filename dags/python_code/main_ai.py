import psycopg2
import snowflake.connector
import os
from dotenv import load_dotenv
import time

load_dotenv()

def get_table_names(pg_cursor, schema_name):
    """Get all table names in a schema"""
    pg_cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
    """, (schema_name,))
    return [row[0] for row in pg_cursor.fetchall()]

def get_create_table_ddl(pg_cursor, schema_name, table_name):
    """Get column definitions from Postgres and convert to Snowflake DDL"""
    pg_cursor.execute("""
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema_name, table_name))

    columns = []
    for col_name, data_type, max_length, is_nullable in pg_cursor.fetchall():
        # Map Postgres types to Snowflake types
        sf_type = map_pg_to_snowflake_type(data_type, max_length)
        nullable = "" if is_nullable == "YES" else "NOT NULL"
        columns.append(f"{col_name} {sf_type} {nullable}".strip())

    return f"CREATE OR REPLACE TABLE {schema_name}.{table_name} ({', '.join(columns)})"

def map_pg_to_snowflake_type(pg_type, max_length):
    """Map Postgres data types to Snowflake data types"""
    type_mapping = {
        'integer': 'INTEGER',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'numeric': 'NUMBER',
        'decimal': 'NUMBER',
        'real': 'FLOAT',
        'double precision': 'FLOAT',
        'character varying': f'VARCHAR({max_length})' if max_length else 'VARCHAR',
        'varchar': f'VARCHAR({max_length})' if max_length else 'VARCHAR',
        'character': f'CHAR({max_length})' if max_length else 'CHAR',
        'char': f'CHAR({max_length})' if max_length else 'CHAR',
        'text': 'VARCHAR',
        'date': 'DATE',
        'timestamp without time zone': 'TIMESTAMP_NTZ',
        'timestamp with time zone': 'TIMESTAMP_TZ',
        'boolean': 'BOOLEAN',
        'json': 'VARIANT',
        'jsonb': 'VARIANT',
    }
    return type_mapping.get(pg_type, 'VARCHAR')

def chunks(lst, n):
    """Yield successive n-sized chunks from lst"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def create_pg_connection():
    """Create a new Postgres connection with keepalive settings"""
    return psycopg2.connect(
        host=os.getenv('PGHOST'),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD'),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        connect_timeout=10
    )

def ensure_pg_connection(pg_conn):
    """Check if connection is alive, reconnect if needed"""
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        return pg_conn
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        print("  Postgres connection lost, reconnecting...")
        return create_pg_connection()

def extract_and_load():
    schema_name = 'employees'

    # Connect to Postgres
    print('Connecting to Postgres...')
    pg_conn = create_pg_connection()
    print('Connected to Postgres.')

    try:
        # Connect to Snowflake
        print('Connecting to Snowflake...')
        with snowflake.connector.connect(
            user=os.getenv('SF_USER'),
            password=os.getenv('SF_PASSWORD'),
            account=os.getenv('SF_ACCOUNT'),
            warehouse=os.getenv('SF_WAREHOUSE'),
            database=os.getenv('SF_DATABASE'),
        ) as sf_conn:
            print('Connected to Snowflake.')

            with pg_conn.cursor() as pg_cursor:
                with sf_conn.cursor() as sf_cursor:
                    # Create schema in Snowflake if not exists
                    sf_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                    print(f"Schema '{schema_name}' ready in Snowflake.")

                    # Get all tables from Postgres schema
                    tables = get_table_names(pg_cursor, schema_name)
                    print(f"Found {len(tables)} tables: {tables}")

                    # TEMP ignore large tables since big data takes too long
                    tables = [t for t in tables if t not in ('salary', 'title', 'department_employee')]

                    # Process each table
                    for table_name in tables:
                        # Ensure connection is alive before processing each table
                        pg_conn = ensure_pg_connection(pg_conn)

                        print(f"\nProcessing table: {table_name}")

                        # Create new cursor after reconnection
                        with pg_conn.cursor() as pg_cursor:
                            # Create/replace table in Snowflake
                            create_ddl = get_create_table_ddl(pg_cursor, schema_name, table_name)
                            sf_cursor.execute(create_ddl)
                            print(f"  Created/replaced table in Snowflake")

                            # Extract data from Postgres
                            pg_cursor.execute(f"SELECT * FROM {schema_name}.{table_name}")
                            rows = pg_cursor.fetchall()
                            print(f"  Extracted {len(rows)} rows from Postgres")

                            if rows:
                                # Get column count and calculate batch size
                                num_columns = len(pg_cursor.description)
                                batch_size = min(10000, 15000 // num_columns)
                                print(f"  Using batch size: {batch_size} (table has {num_columns} columns)")

                                columns = [desc[0] for desc in pg_cursor.description]
                                placeholders = ', '.join(['%s'] * num_columns)
                                insert_sql = f"INSERT INTO {schema_name}.{table_name} VALUES ({placeholders})"

                                # Insert in batches
                                total_inserted = 0
                                for batch in chunks(rows, batch_size):
                                    sf_cursor.executemany(insert_sql, batch)
                                    total_inserted += len(batch)
                                    print(f"  Loaded {total_inserted}/{len(rows)} rows...", end='\r')

                                print(f"  Loaded {total_inserted} rows into Snowflake ✓")
                            else:
                                print(f"  No data to load (empty table)")

                    print("\n✅ All tables processed successfully!")

    finally:
        # Ensure Postgres connection is closed
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
            print("\nPostgres connection closed.")

if __name__ == '__main__':
    extract_and_load()
