"""
Airflow‑safe Spark Postgres → Snowflake transfer module

Key changes vs. your original:
- No side‑effects at import time (no dotenv load, no prints).
- Pure library on import; execution only via __main__ or airflow_entry().
- Added airflow_entry(**context) for PythonOperator use.
- argparse uses parse_known_args() in __main__ to avoid crashes if unknown
  args leak in during local shell invocations.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Tuple, Optional

# NOTE: Do NOT import heavy libs or load .env at import time in Airflow context
# (scheduler imports this file). pyspark/snowflake are imported inside functions
# right before use to avoid import‑time side effects and speed up DAG parsing.

# -----------------------------------------------------------------------------
# Type mapping & SQL helpers (safe at import time)
# -----------------------------------------------------------------------------

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
    pieces = []
    for name, pg_type, max_len, is_null in cols:
        sf_type = map_pg_to_snowflake_type(pg_type, max_len)
        nullable = "" if (is_null or "").upper() == "YES" else "NOT NULL"
        pieces.append(f"{name} {sf_type} {nullable}".strip())
    return f"CREATE OR REPLACE TABLE {schema}.{table} ({', '.join(pieces)})"


# -----------------------------------------------------------------------------
# Lazily imported execution helpers
# -----------------------------------------------------------------------------

def _jdbc_read(spark, url: str, user: str, password: str, query: str, driver: str = "org.postgresql.Driver"):
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .option("dbtable", f"({query}) as subq")
        .load()
    )


def get_table_names(spark, pg_url: str, pg_user: str, pg_password: str, schema_name: str) -> List[str]:
    q = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    df = _jdbc_read(spark, pg_url, pg_user, pg_password, q)
    return [r["table_name"] for r in df.collect()]


def get_columns(spark, pg_url: str, pg_user: str, pg_password: str, schema_name: str, table: str):
    q = f"""
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    return [
        (r["column_name"], r["data_type"], r["character_maximum_length"], r["is_nullable"])
        for r in _jdbc_read(spark, pg_url, pg_user, pg_password, q).collect()
    ]


def get_min_max_for_column(spark, pg_url: str, pg_user: str, pg_password: str, schema: str, table: str, col: str):
    q = f"SELECT MIN({col}) AS lo, MAX({col}) AS hi FROM {schema}.{table}"
    row = _jdbc_read(spark, pg_url, pg_user, pg_password, q).collect()[0]
    return row["lo"], row["hi"]


def pick_partition_column(spark, pg_url: str, pg_user: str, pg_password: str, schema: str, table: str) -> Optional[str]:
    q = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    rows = _jdbc_read(spark, pg_url, pg_user, pg_password, q).collect()
    intish = {"integer", "bigint", "smallint"}
    for r in rows:
        if r["data_type"] in intish and (r["column_name"] == "id" or r["column_name"].endswith("_id")):
            return r["column_name"]
    for r in rows:
        if r["data_type"] in intish:
            return r["column_name"]
    return None


def exec_in_snowflake(ddl: str, sf_user: str, sf_password: str, sf_account: str, sf_warehouse: str, sf_database: str, sf_schema: str, sf_role: Optional[str]):
    import snowflake.connector

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


# -----------------------------------------------------------------------------
# Core runner (no argparse here)
# -----------------------------------------------------------------------------

def run_transfer(
    schema: str,
    pg_url: str,
    pg_user: str,
    pg_password: str,
    pg_sslmode: Optional[str],
    sf_account: str,
    sf_user: str,
    sf_password: str,
    sf_warehouse: str,
    sf_database: str,
    sf_schema: Optional[str],
    sf_role: Optional[str],
    default_num_partitions: int = 64,
    fetchsize: int = 10_000,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    write_mode: str = "overwrite",
    truncate_table: str = "on",
):
    from pyspark.sql import SparkSession  # lazy import

    spark = (
        SparkSession.builder
        .appName("psql-to-snowflake")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.4," \
            "net.snowflake:spark-snowflake_2.13:3.1.5," \
            "net.snowflake:snowflake-jdbc:3.24.2"
        )
        .config("spark.sql.shuffle.partitions", "256")
        .getOrCreate()
    )

    final_sf_schema = sf_schema or schema

    # Ensure destination schema exists
    exec_in_snowflake(
        f"CREATE SCHEMA IF NOT EXISTS {final_sf_schema}",
        sf_user, sf_password, sf_account, sf_warehouse, sf_database, final_sf_schema, sf_role,
    )
    print(f"Snowflake schema ready: {final_sf_schema}")

    # Discover tables
    tables = get_table_names(spark, pg_url, pg_user, pg_password, schema)

    if include:
        incl = {t.strip() for t in include if t and t.strip()}
        if incl:
            tables = [t for t in tables if t in incl]
    if exclude:
        excl = {t.strip() for t in exclude if t and t.strip()}
        if excl:
            tables = [t for t in tables if t not in excl]

    print(f"Found {len(tables)} tables in schema '{schema}': {tables}")

    sfOptions = {
        "sfURL": f"{sf_account}.snowflakecomputing.com",
        "sfUser": sf_user,
        "sfPassword": sf_password,
        "sfDatabase": sf_database,
        "sfSchema": final_sf_schema,
        "sfWarehouse": sf_warehouse,
    }
    if sf_role:
        sfOptions["sfRole"] = sf_role

    # Example: temporary filter—remove if not desired
    tables = [t for t in tables if t not in ("salary", "title", "department_employee")]

    for table in tables:
        full_src = f"{schema}.{table}"
        full_dst = f"{final_sf_schema}.{table}"
        print(f"\n=== Processing table: {full_src} -> {full_dst} ===")

        cols = get_columns(spark, pg_url, pg_user, pg_password, schema, table)
        if not cols:
            print(f"  Skipping (no columns): {full_src}")
            continue

        ddl = build_snowflake_create(final_sf_schema, table, cols)
        exec_in_snowflake(ddl, sf_user, sf_password, sf_account, sf_warehouse, sf_database, final_sf_schema, sf_role)
        print("  Created/Replaced table in Snowflake.")

        reader = (
            spark.read.format("jdbc")
            .option("url", pg_url)
            .option("user", pg_user)
            .option("password", pg_password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", int(fetchsize))
        )

        df = None
        part_col = pick_partition_column(spark, pg_url, pg_user, pg_password, schema, table)
        if part_col:
            try:
                lo, hi = get_min_max_for_column(spark, pg_url, pg_user, pg_password, schema, table, part_col)
                if lo is not None and hi is not None and lo != hi:
                    print(f"  Parallelizing read on {part_col} [{lo}, {hi}] with {default_num_partitions} partitions")
                    df = (
                        reader.option("dbtable", full_src)
                              .option("partitionColumn", part_col)
                              .option("lowerBound", str(lo))
                              .option("upperBound", str(hi))
                              .option("numPartitions", int(default_num_partitions))
                              .load()
                    )
            except Exception as e:
                print(f"  Partition bounds failed on {part_col}: {e}")

        if df is None:
            print("  Falling back to non-partitioned JDBC read")
            df = reader.option("dbtable", full_src).load()

        ordered_cols = [c[0] for c in cols]
        existing_cols = df.columns
        select_cols = [c for c in ordered_cols if c in existing_cols]
        df = df.select(*select_cols)

        (
            df.write.format("snowflake")
              .options(**sfOptions)
              .option("dbtable", full_dst)
              .option("truncate_table", truncate_table)
              .mode(write_mode)
              .save()
        )
        print(f"  Loaded {df.count()} rows into {full_dst} ✓")

    print("\n✅ All tables processed successfully!")


# -----------------------------------------------------------------------------
# Airflow entrypoint (call from a PythonOperator)
# -----------------------------------------------------------------------------

def airflow_entry(**context):
    """PythonOperator entrypoint. Read params/env and run transfer.

    Usage in DAG:
        from airflow.operators.python import PythonOperator
        transfer = PythonOperator(
            task_id="psql_to_snowflake",
            python_callable=airflow_entry,
        )
    Provide params via Airflow Variables/params or env.
    """
    params = (context or {}).get("params", {})

    def _list_from_env(name: str) -> Optional[List[str]]:
        v = os.getenv(name, "").strip()
        return [x.strip() for x in v.split(",") if x.strip()] if v else None

    run_transfer(
        schema=params.get("schema") or os.getenv("PG_SCHEMA", "employees"),
        pg_url=params.get("pg_url") or os.getenv("PG_JDBC_URL"),
        pg_user=params.get("pg_user") or os.getenv("PGUSER"),
        pg_password=params.get("pg_password") or os.getenv("PGPASSWORD"),
        pg_sslmode=params.get("pg_sslmode") or os.getenv("PGSSLMODE", "require"),
        sf_account=params.get("sf_account") or os.getenv("SF_ACCOUNT"),
        sf_user=params.get("sf_user") or os.getenv("SF_USER"),
        sf_password=params.get("sf_password") or os.getenv("SF_PASSWORD"),
        sf_warehouse=params.get("sf_warehouse") or os.getenv("SF_WAREHOUSE"),
        sf_database=params.get("sf_database") or os.getenv("SF_DATABASE"),
        sf_schema=params.get("sf_schema") or os.getenv("SF_SCHEMA"),
        sf_role=params.get("sf_role") or os.getenv("SF_ROLE"),
        default_num_partitions=int(params.get("default_num_partitions") or os.getenv("SPARK_DEFAULT_NUM_PARTITIONS", "64")),
        fetchsize=int(params.get("fetchsize") or os.getenv("PG_FETCHSIZE", "10000")),
        include=params.get("include") or _list_from_env("INCLUDE_TABLES"),
        exclude=params.get("exclude") or _list_from_env("EXCLUDE_TABLES"),
        write_mode=params.get("write_mode") or os.getenv("SF_WRITE_MODE", "overwrite"),
        truncate_table=params.get("truncate_table") or os.getenv("SF_TRUNCATE_TABLE", "on"),
    )


# -----------------------------------------------------------------------------
# Local/CLI entrypoint (spark-submit or python)
# -----------------------------------------------------------------------------

def _load_dotenv_if_present():
    """Load .env only for CLI runs (never during Airflow import)."""
    env_path_candidates = [
        Path(__file__).resolve().parent.parent / ".env",  # repo/.env
        Path("/opt/airflow/dags/.env"),                    # optional in Airflow image
    ]
    for p in env_path_candidates:
        try:
            if p.exists():
                from dotenv import load_dotenv
                load_dotenv(p)
                print(f"✅ Loaded .env from {p}")
                break
        except Exception:
            pass


def _parse_cli_args(argv: Optional[List[str]] = None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default=os.getenv("PG_SCHEMA", "employees"))
    ap.add_argument("--pg-url", default=os.getenv("PG_JDBC_URL"))
    ap.add_argument("--pg-user", default=os.getenv("PGUSER"))
    ap.add_argument("--pg-password", default=os.getenv("PGPASSWORD"))
    ap.add_argument("--pg-sslmode", default=os.getenv("PGSSLMODE", "require"))
    ap.add_argument("--sf-account", default=os.getenv("SF_ACCOUNT"))
    ap.add_argument("--sf-user", default=os.getenv("SF_USER"))
    ap.add_argument("--sf-password", default=os.getenv("SF_PASSWORD"))
    ap.add_argument("--sf-warehouse", default=os.getenv("SF_WAREHOUSE"))
    ap.add_argument("--sf-database", default=os.getenv("SF_DATABASE"))
    ap.add_argument("--sf-schema", default=os.getenv("SF_SCHEMA"))
    ap.add_argument("--sf-role", default=os.getenv("SF_ROLE"))
    ap.add_argument("--default-num-partitions", type=int, default=int(os.getenv("SPARK_DEFAULT_NUM_PARTITIONS", "64")))
    ap.add_argument("--fetchsize", type=int, default=int(os.getenv("PG_FETCHSIZE", "10000")))
    ap.add_argument("--exclude", nargs="*", default=os.getenv("EXCLUDE_TABLES", "").split(",") if os.getenv("EXCLUDE_TABLES") else [])
    ap.add_argument("--include", nargs="*", default=os.getenv("INCLUDE_TABLES", "").split(",") if os.getenv("INCLUDE_TABLES") else [])
    ap.add_argument("--write-mode", default=os.getenv("SF_WRITE_MODE", "overwrite"))
    ap.add_argument("--truncate_table", default=os.getenv("SF_TRUNCATE_TABLE", "on"))
    # parse_known_args avoids crashing if the invoking shell injects unrelated args
    args, _ = ap.parse_known_args(argv)
    return args


def _validate_required(args) -> None:
    required = {
        "pg-url": args.pg_url,
        "pg-user": args.pg_user,
        "pg-password": args.pg_password,
        "sf-account": args.sf_account,
        "sf-user": args.sf_user,
        "sf-password": args.sf_password,
        "sf-warehouse": args.sf_warehouse,
        "sf-database": args.sf_database,
        "sf-schema": args.sf_schema or args.schema,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"Missing required args/env: {missing}", file=sys.stderr)
        sys.exit(2)


def main(argv: Optional[List[str]] = None):
    _load_dotenv_if_present()
    args = _parse_cli_args(argv)
    _validate_required(args)

    run_transfer(
        schema=args.schema,
        pg_url=args.pg_url,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
        pg_sslmode=args.pg_sslmode,
        sf_account=args.sf_account,
        sf_user=args.sf_user,
        sf_password=args.sf_password,
        sf_warehouse=args.sf_warehouse,
        sf_database=args.sf_database,
        sf_schema=args.sf_schema,
        sf_role=args.sf_role,
        default_num_partitions=args.default_num_partitions,
        fetchsize=args.fetchsize,
        include=args.include,
        exclude=args.exclude,
        write_mode=args.write_mode,
        truncate_table=args.truncate_table,
    )


if __name__ == "__main__":
    main()
