# create an EL process that extracts data from postgres db hosted on Neon db and loads it into snowflake
# first iteration should just do a read all from source and overwrite all in destination
import psycopg2
import os
from dotenv import load_dotenv


# load env varialbles from .env file
load_dotenv()

def run_script():

    print('Connecting to Postgres database...')

    with psycopg2.connect(
        host=os.getenv('PGHOST'),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD'),
        port=os.getenv('PGPORT', 5432),
        sslmode=os.getenv('PGSSLMODE'),
        channel_binding=os.getenv('PGCHANNELBINDING'),
    ) as pg_conn:

        print('Connected to Postgres database.')

        # create a cursor
        with pg_conn.cursor() as pg_cursor:

            # execute a query to fetch all data from a table
            pg_cursor.execute('SELECT count(*) FROM employees.employee;')

            print(pg_cursor.fetchall())

    print('Postgres connection closed.')

if __name__ == '__main__':
    run_script()
