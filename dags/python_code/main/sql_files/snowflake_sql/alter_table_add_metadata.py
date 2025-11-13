def alter_table_add_metadata(schema_name, table_name):
    """ Add columns to tables for loaded timestamp and source file name. """
    sql = f"""
        ALTER TABLE {schema_name}.{table_name}
        ADD COLUMN _etl_loaded_timestamp TIMESTAMP_LTZ,
                    _etl_source_file VARCHAR;
    """
