def create_table(schema_name, table_name, table_files_path):
	""" Create table, infer schema based on file content """
	
	sql = f"""
		CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}
		USING TEMPLATE (
			SELECT
				ARRAY_AGG(OBJECT_CONSTRUCT(*)),
				_etl_loaded_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
				_etl_source_file VARCHAR
			FROM TABLE(
				INFER_SCHEMA(
				LOCATION => '{table_files_path}',
				FILE_FORMAT => 'PARQUET_FORMAT'
				)
			)
		);
	"""

	print(sql)

	return sql
