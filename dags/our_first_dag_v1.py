# import the libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.utils.dates import days_ago


# defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Nuttawich T.',
    # 'start_date': days_ago(0),
    # 'email': ['nuttawich@somemail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


# define the DAG
with DAG(
    dag_id='our_first_dag_v4',
    default_args=default_args,
    description='This is the first dag that I write',
    schedule_interval='@daily',  # '@daily'==timedelta(days=1)
    start_date=datetime(2022, 7, 22, 21)

) as dag:
    # define the tasks
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo Hello world, this is the first task!'
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo Wow, I am the second task!, will be running after task1'
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo Hi, third task is here, supposed to run after task1, at the same time of task 2'
    )

    # task pipeline
    task1 >> [task2, task3]
