def get_all_schemas_and_tables():
    sql = f"""
        SELECT
            DISTINCT
            table_schema,
            table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
            AND table_schema not in (
                'information_schema',
                'pg_catalog'
            )
        ;
    """
    return sql
