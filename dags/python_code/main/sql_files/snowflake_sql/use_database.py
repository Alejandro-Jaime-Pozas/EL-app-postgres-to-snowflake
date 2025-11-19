def use_database(database_name: str):
    """ Use a database. """

    sql = f"""
        USE DATABASE {database_name};
    """

    print(sql)

    return sql 
