from datetime import datetime, timedelta

from python_code.main.utils import default_args

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


# default_args = {
#     'owner': 'alex',
#     'retries': 5,
#     'retry_delay': timedelta(minutes=2)
# }

with DAG(
    dag_id='our_first_dag_v04',
    default_args=default_args,
    description='This is our first DAG',
    start_date=datetime(2025, 10, 20, 2),
    schedule='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is the first task!',
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo hello world, this is the second task!',
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo hello world, this is the third task and running parallel to task2!',
    )

    task1 >> [task2, task3]
