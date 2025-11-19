def create_database(database_name: str):
    """ Create a database if not exists based on input name. """
    
    sql = f"""
        CREATE DATABASE IF NOT EXISTS {database_name};
    """

    print(sql)

    return sql
