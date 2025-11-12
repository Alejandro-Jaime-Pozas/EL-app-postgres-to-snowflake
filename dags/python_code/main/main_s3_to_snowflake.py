# Functionality related to transferring s3 files to snowflake.

# No need for s3 queue check nor pipe since airflow tasks handle execution
# Will use snowflake hook for connection from airflow
# Will use infer schema to auto-assign column names and data types for tables
# Will use snowflake external stage to link s3 bucket to snowflake for instant access/bucket updates
# Will use normal snowflake tables, not external to dump data since normal tables more efficient

# STEPS
    # get list of schemas-tables from s3 upload
    # loop through each schema-table
    # check if table exists in snowflake
        # if not exists, then create the table
    # copy data into table (dont worry about row updates or deletes for now just inserts)


# 1. Create snowflake tables if don't exist using correct column names and data types based on s3 bucket contents

# 2. After table is created, copy only new parquet files into the table (snowflake handles parallel processing and prevents prev file loads)
	# use snowpipe to auto-load as soon as data available in s3 into snowflake.

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = 'snowflake-sandiego-employees-db'


def get_hook(snowflake_conn_id=SNOWFLAKE_CONN_ID or None):
    """ Get the airflow snowflake hook. """
    


def copy_s3_data_into_snowflake(
    schema_name: str = None,
    table_name: str = None,
):
    """
    Extracts data from a single s3 table run path
    and copies into snowflake relevant table.
    """
