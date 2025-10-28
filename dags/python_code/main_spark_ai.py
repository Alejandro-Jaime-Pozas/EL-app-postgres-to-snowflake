# python script to extract data from postgres and load to snowflake using spark

# psql_to_sf_spark.py
# Run with (example):
# spark-submit \
#  --packages org.postgresql:postgresql:42.7.4,net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,net.snowflake:snowflake-jdbc:3.16.1 \
#  --conf spark.sql.shuffle.partitions=256 \
#  /opt/spark/jobs/psql_to_sf_spark.py \
#    --schema employees \
#    --pg-url "jdbc:postgresql://$PGHOST:5432/$PGDATABASE" \
#    --pg-user "$PGUSER" --pg-password "$PGPASSWORD" --pg-sslmode "$PGSSLMODE" \
#    --sf-account "$SF_ACCOUNT" --sf-user "$SF_USER" --sf-password "$SF_PASSWORD" \
#    --sf-warehouse "$SF_WAREHOUSE" --sf-database "$SF_DATABASE" --sf-role "$SF_ROLE"

import os
import sys
import argparse
from pathlib import Path
from typing import List, Tuple, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# Optional: load .env from Airflow DAGs directory if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path("/opt/airflow/dags/.env"))
except Exception:
    pass

# ---- PG -> Snowflake type mapping (similar to your original) ----
def map_pg_to_snowflake_type(pg_type: str, max_length: Optional[int]) -> str:
    pg_type = (pg_type or "").lower()
    mapping = {
        "integer": "INTEGER",
        "int": "INTEGER",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "numeric": "NUMBER",
        "decimal": "NUMBER",
        "real": "FLOAT",
        "double precision": "FLOAT",
        "text": "VARCHAR",
        "date": "DATE",
        "timestamp without time zone": "TIMESTAMP_NTZ",
        "timestamp with time zone": "TIMESTAMP_TZ",
        "boolean": "BOOLEAN",
        "json": "VARIANT",
        "jsonb": "VARIANT",
        "character varying": f"VARCHAR({max_length})" if max_length else "VARCHAR",
        "varchar": f"VARCHAR({max_length})" if max_length else "VARCHAR",
        "character": f"CHAR({max_length})" if max_length else "CHAR",
        "char": f"CHAR({max_length})" if max_length else "CHAR",
    }
    return mapping.get(pg_type, "VARCHAR")

def build_snowflake_create(schema: str, table: str, cols: List[Tuple[str, str, Optional[int], str]]) -> str:
    """
    cols: list of (column_name, data_type, character_maximum_length, is_nullable)
    """
    pieces = []
    for name, pg_type, max_len, is_null in cols:
        sf_type = map_pg_to_snowflake_type(pg_type, max_len)
        nullable = "" if (is_null or "").upper() == "YES" else "NOT NULL"
        pieces.append(f'{name} {sf_type} {nullable}'.strip())
    return f"CREATE OR REPLACE TABLE {schema}.{table} ({', '.join(pieces)})"

# ---- Helpers to query Postgres information_schema via Spark JDBC ----
def jdbc_read(spark: SparkSession, url: str, user: str, password: str, query: str, driver: str = "org.postgresql.Driver"):
    """Helpers to query Postgres information_schema via Spark JDBC."""
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .option("dbtable", f"({query}) as subq")
        .load()
    )

def get_table_names(spark: SparkSession, pg_url: str, pg_user: str, pg_password: str, schema_name: str) -> List[str]:
    q = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    df = jdbc_read(spark, pg_url, pg_user, pg_password, q)
    return [r["table_name"] for r in df.collect()]

def get_columns(spark: SparkSession, pg_url: str, pg_user: str, pg_password: str, schema_name: str, table: str):
    q = f"""
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    return [
        (r["column_name"], r["data_type"], r["character_maximum_length"], r["is_nullable"])
        for r in jdbc_read(spark, pg_url, pg_user, pg_password, q).collect()
    ]

def get_min_max_for_column(spark: SparkSession, pg_url: str, pg_user: str, pg_password: str, schema: str, table: str, col: str):
    q = f"SELECT MIN({col}) AS lo, MAX({col}) AS hi FROM {schema}.{table}"
    row = jdbc_read(spark, pg_url, pg_user, pg_password, q).collect()[0]
    return row["lo"], row["hi"]

def pick_partition_column(
    spark: SparkSession,
    pg_url: str,
    pg_user: str,
    pg_password: str,
    schema: str,
    table: str,
) -> Optional[str]:
    """
    Heuristics: prefer integer-type column named 'id' or ending with '_id'.
    Fallback to first integer-like column. Return None if none suitable.
    """
    q = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    rows = jdbc_read(spark, pg_url, pg_user, pg_password, q).collect()
    intish = {"integer", "bigint", "smallint"}
    # name-based preference
    for r in rows:
        if r["data_type"] in intish and (r["column_name"] == "id" or r["column_name"].endswith("_id")):
            return r["column_name"]
    # any integer-like
    for r in rows:
        if r["data_type"] in intish:
            return r["column_name"]
    return None

# ---- Snowflake utilities (DDL execution from driver) ----
# We'll run DDL using the official Snowflake Python connector from the Spark driver.
# This keeps the data path distributed (Spark) while letting us control table shapes.
def exec_in_snowflake(ddl: str, sf_user: str, sf_password: str, sf_account: str, sf_warehouse: str, sf_database: str, sf_schema: str, sf_role: Optional[str]):
    import snowflake.connector  # lazy import on driver
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
        role=sf_role if sf_role else None,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
    finally:
        conn.close()

def parse_args():
    ap = argparse.ArgumentParser()
    # Core
    ap.add_argument("--schema", default=os.getenv("PG_SCHEMA", "employees"))
    # Postgres
    ap.add_argument("--pg-url", default=os.getenv("PG_JDBC_URL"))  # e.g., jdbc:postgresql://host:5432/db
    ap.add_argument("--pg-user", default=os.getenv("PGUSER"))
    ap.add_argument("--pg-password", default=os.getenv("PGPASSWORD"))
    ap.add_argument("--pg-sslmode", default=os.getenv("PGSSLMODE", "require"))  # optional, for visibility
    # Snowflake
    ap.add_argument("--sf-account", default=os.getenv("SF_ACCOUNT"))
    ap.add_argument("--sf-user", default=os.getenv("SF_USER"))
    ap.add_argument("--sf-password", default=os.getenv("SF_PASSWORD"))
    ap.add_argument("--sf-warehouse", default=os.getenv("SF_WAREHOUSE"))
    ap.add_argument("--sf-database", default=os.getenv("SF_DATABASE"))
    ap.add_argument("--sf-schema", default=os.getenv("SF_SCHEMA"))  # destination schema in Snowflake
    ap.add_argument("--sf-role", default=os.getenv("SF_ROLE"))
    # Tuning
    ap.add_argument("--default-num-partitions", type=int, default=int(os.getenv("SPARK_DEFAULT_NUM_PARTITIONS", "64")))
    ap.add_argument("--fetchsize", type=int, default=int(os.getenv("PG_FETCHSIZE", "10000")))
    ap.add_argument("--exclude", nargs="*", default=os.getenv("EXCLUDE_TABLES", "").split(",") if os.getenv("EXCLUDE_TABLES") else [])
    ap.add_argument("--include", nargs="*", default=os.getenv("INCLUDE_TABLES", "").split(",") if os.getenv("INCLUDE_TABLES") else [])
    ap.add_argument("--write-mode", default=os.getenv("SF_WRITE_MODE", "overwrite"))  # overwrite|append
    ap.add_argument("--truncate_table", default=os.getenv("SF_TRUNCATE_TABLE", "on"))  # on|off
    return ap.parse_args()

def main():
    args = parse_args()

    # Validate required args
    required = {
        "pg-url": args.pg_url,
        "pg-user": args.pg_user,
        "pg-password": args.pg_password,
        "sf-account": args.sf_account,
        "sf-user": args.sf_user,
        "sf-password": args.sf_password,
        "sf-warehouse": args.sf_warehouse,
        "sf-database": args.sf_database,
        "sf-schema": args.sf_schema or args.schema,  # allow defaulting to same name
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"Missing required args/env: {missing}", file=sys.stderr)
        sys.exit(2)

    # Spark session
    spark = (
        SparkSession.builder.appName("psql-to-snowflake")
        .getOrCreate()
    )

    # Final Snowflake schema to use (default to --schema if --sf-schema not set)
    sf_schema = args.sf_schema or args.schema

    # Ensure destination schema exists
    exec_in_snowflake(
        f'CREATE SCHEMA IF NOT EXISTS {sf_schema}',
        args.sf_user, args.sf_password, args.sf_account, args.sf_warehouse, args.sf_database, sf_schema, args.sf_role
    )
    print(f"Snowflake schema ready: {sf_schema}")

    # List tables
    tables = get_table_names(spark, args.pg_url, args.pg_user, args.pg_password, args.schema)
    if args.include and any(t for t in args.include):
        incl = set([t.strip() for t in args.include if t.strip()])
        tables = [t for t in tables if t in incl]
    if args.exclude and any(t for t in args.exclude):
        excl = set([t.strip() for t in args.exclude if t.strip()])
        tables = [t for t in tables if t not in excl]

    print(f"Found {len(tables)} tables in schema '{args.schema}': {tables}")

    # Snowflake connector options for Spark
    sfOptions = {
        "sfURL": f"{args.sf_account}.snowflakecomputing.com",
        "sfUser": args.sf_user,
        "sfPassword": args.sf_password,
        "sfDatabase": args.sf_database,
        "sfSchema": sf_schema,
        "sfWarehouse": args.sf_warehouse,
    }
    if args.sf_role:
        sfOptions["sfRole"] = args.sf_role

    # Process each table
    for table in tables:
        full_src = f"{args.schema}.{table}"
        full_dst = f"{sf_schema}.{table}"
        print(f"\n=== Processing table: {full_src} -> {full_dst} ===")

        # 1) Build & execute Snowflake DDL to align types
        cols = get_columns(spark, args.pg_url, args.pg_user, args.pg_password, args.schema, table)
        if not cols:
            print(f"  Skipping (no columns): {full_src}")
            continue
        ddl = build_snowflake_create(sf_schema, table, cols)
        exec_in_snowflake(ddl, args.sf_user, args.sf_password, args.sf_account, args.sf_warehouse, args.sf_database, sf_schema, args.sf_role)
        print("  Created/Replaced table in Snowflake.")

        # 2) Decide partitioning for parallel JDBC read
        partition_col = pick_partition_column(spark, args.pg_url, args.pg_user, args.pg_password, args.schema, table)
        reader = (
            spark.read.format("jdbc")
            .option("url", args.pg_url)
            .option("user", args.pg_user)
            .option("password", args.pg_password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", args.fetchsize)
        )

        df = None
        if partition_col:
            try:
                lo, hi = get_min_max_for_column(spark, args.pg_url, args.pg_user, args.pg_password, args.schema, table, partition_col)
                if lo is not None and hi is not None and lo != hi:
                    print(f"  Parallelizing read on {partition_col} [{lo}, {hi}] with {args.default_num_partitions} partitions")
                    df = (
                        reader.option("dbtable", full_src)
                              .option("partitionColumn", partition_col)
                              .option("lowerBound", str(lo))
                              .option("upperBound", str(hi))
                              .option("numPartitions", str(args.default_num_partitions))
                              .load()
                    )
            except Exception as e:
                print(f"  Partition bounds failed on {partition_col}: {e}")

        if df is None:
            print("  Falling back to non-partitioned JDBC read")
            df = reader.option("dbtable", full_src).load()

        # 3) Optional: basic column order match (Snowflake INSERT relies on order if no explicit column list)
        #    We'll align columns to information_schema order from PG to be explicit.
        ordered_cols = [c[0] for c in cols]
        existing_cols = df.columns
        select_cols = [c for c in ordered_cols if c in existing_cols]
        df = df.select(*select_cols)

        # 4) Write to Snowflake in parallel
        writer = (
            df.write.format("snowflake")
              .options(**sfOptions)
              .option("dbtable", full_dst)
              .option("truncate_table", args.truncate_table)  # if mode="overwrite", this keeps table and truncates
        )

        # Use overwrite (truncate in-place) or append depending on args
        writer.mode(args.write_mode).save()
        print(f"  Loaded {df.count()} rows into {full_dst} ✓")

    print("\n✅ All tables processed successfully!")

if __name__ == "__main__":
    main()
