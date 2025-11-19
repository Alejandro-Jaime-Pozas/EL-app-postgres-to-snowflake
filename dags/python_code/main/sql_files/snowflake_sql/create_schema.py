def create_schema(schema_name: str):
    """ Create the schema if it doesn't exist. """

    # TODO implement this error check later for other objects as well..
    if not schema_name:
        raise ValueError('You must provide a schema name to create a schema.')

    sql = f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
    """

    print(sql)

    return sql
