from datetime import timedelta


default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
