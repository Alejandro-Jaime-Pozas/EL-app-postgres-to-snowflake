from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


PG_CONN_ID='pg-local'
AWS_CONN_ID='aws-sandiego'


# Issue is this runs everytime connects to aws/pg and only writes one file..look into this
def create_extract_task(schema_name: str, table_name: str):
    return SqlToS3Operator(
        task_id=f'extract_{schema_name}_{table_name}',
        sql_conn_id=PG_CONN_ID,
        query=f"SELECT * FROM {schema_name}.{table_name}",
        s3_bucket='postgres-neon-db-employees',
        s3_key=f'db-data/schemas/{schema_name}/tables/{table_name}/{{{{ ds }}}}/data.parquet',
        aws_conn_id=AWS_CONN_ID,
        file_format='parquet',
        replace=True,
    )
