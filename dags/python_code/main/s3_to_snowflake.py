# Functionality related to transferring s3 files to snowflake.

# 1. for each new s3 file upload (can pass in schema.table from xcom) check if table exists, if not create

# 2. after table is created, copy only new parquet files into the table (snowflake handles parallel processing and prevents prev file loads)
	# use snowpipe to auto-load as soon as data available in s3 into snowflake.


def map_pg_to_snowflake_datatypes():
    """ Make postgres incomding data types snowflake-proof. """
    # maybe this is not needed since snowflake can infer schemas!


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


# Create a pipe that loads all the data into columns in the target table that match corresponding columns represented in the data. Column names are case-insensitive.
# In addition, load metadata from the METADATA$START_SCAN_TIME and METADATA$FILENAME metadata columns to the columns named c1 and c2.
"""CREATE PIPE mypipe3
  AS
  (COPY INTO mytable
    FROM @mystage
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    INCLUDE_METADATA = (c1= METADATA$START_SCAN_TIME, c2=METADATA$FILENAME)
    FILE_FORMAT = (TYPE = 'JSON'));"""


# Create a pipe in the current schema for automatic loading of data using event notifications received from a messaging service:
"""CREATE PIPE mypipe_s3
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:001234567890:s3_mybucket'
  AS
  COPY INTO snowpipe_db.public.mytable
  FROM @snowpipe_db.public.mystage
  FILE_FORMAT = (TYPE = 'JSON');"""
