def create_external_stage(
    stg_name: str,
    storage_integration_name: str,
    external_url: str,
    ff_name: str,
):
    """
    Creates an external stage in Snowflake if it does not already exist.

    Args:
        stg_name (str): Name of the stage to create.
        storage_integration_name (str): Name of the Snowflake storage integration.
        external_url (str): URL of the external storage location
            (e.g., ``s3://my-bucket/path/``).
        ff_name (str): Name of an existing file format to use when copying into Snowflake.

    Returns:
        str: The generated SQL statement.

    Check Snowflake docs for more details.
    """

    sql = f"""
        CREATE STAGE IF NOT EXISTS {stg_name}
            STORAGE_INTEGRATION = {storage_integration_name}
            URL = '{external_url}'
            FILE_FORMAT = {ff_name};
    """

    print(sql)

    return sql
