def use_schema(schema_name: str):
    """ Use a schema. """

    sql = f"""
        USE SCHEMA {schema_name};
    """

    print(sql)

    return sql
