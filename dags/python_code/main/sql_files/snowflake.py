def map_pg_to_snowflake_datatypes():
    """ Make postgres incomding data types snowflake-proof. """
    # maybe this is not needed since snowflake can infer schema!


# Create table, infer schema based on file content
"""CREATE TABLE mytable
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@mystage/json/',
          FILE_FORMAT=>'my_json_format'
        )
      ));"""


# Copy data from parquet file to existing snowflake table
"""COPY INTO mytable FROM @mystage/csv/
  FILE_FORMAT = (
    FORMAT_NAME= 'my_csv_format'
  )
  MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;"""
