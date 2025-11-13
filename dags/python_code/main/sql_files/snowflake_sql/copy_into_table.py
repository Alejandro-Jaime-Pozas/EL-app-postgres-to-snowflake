def copy_into_table(schema_name, table_name, table_files_path):
    """ Copy data from an external stage into a table. """

    sql = f"""
        COPY INTO {schema_name}.{table_name}
        FROM {table_files_path}
        FILE_FORMAT = 'PARQUET_FORMAT'
        INCLUDE_METADATA = (_etl_loaded_timestamp= METADATA$START_SCAN_TIME, _etl_source_file=METADATA$FILENAME) -- check if works
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
    """

    print(sql)

    return sql
