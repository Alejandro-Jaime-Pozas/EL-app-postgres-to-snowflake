
# Copy data from parquet file to existing snowflake table
"""COPY INTO mytable FROM @mystage/csv/
  FILE_FORMAT = <my_file_format_name>
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
