def get_all_table_data(schema_name, table_name):
    sql = f"""
        SELECT *
        FROM {schema_name}.{table_name}
        ORDER BY 1;
    """
    return sql
