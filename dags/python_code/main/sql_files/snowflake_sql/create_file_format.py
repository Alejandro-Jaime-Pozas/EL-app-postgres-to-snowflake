def create_file_format(ff_name: str, ff_type: str):
    """ Create a file format if not exists. """

    sql = f"""
        CREATE FILE FORMAT IF NOT EXISTS {ff_name}
        TYPE = {ff_type};
    """

    print(sql)

    return sql
