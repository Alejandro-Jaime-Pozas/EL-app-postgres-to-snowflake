def create_schema(schema_name: str = None):
    """ Create the schema if it doesn't exist. """

    sql = f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
    """

    print(sql)
    
    return sql
