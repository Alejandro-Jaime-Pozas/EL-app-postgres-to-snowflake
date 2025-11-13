def create_table(schema_name, table_name, table_files_path):
	""" Create table, infer schema based on file content """

	sql = f"""
		CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}
		USING TEMPLATE (
			SELECT
				ARRAY_AGG(OBJECT_CONSTRUCT(*))
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
