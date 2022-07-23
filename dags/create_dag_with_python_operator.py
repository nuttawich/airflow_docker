# import the libraries
from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Nuttawich T.',
    'start_date': datetime(2022, 7, 22, 21),  # days_ago(0)==datetime(2022, 7, 22, 21)
    # 'email': ['nuttawich@somemail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


# define the function
def greet(age, ti):
    # pull return value from other tasks
    name = ti.xcom_pull(task_ids='get_name')
    print(f"My name is {name}, and I am {age} years old.")

def get_name_func():
    return "Jacob"


# define the DAG
with DAG(
    dag_id='my_dag_with_python_operator_v8',
    default_args=default_args,
    description='This is the first dag using python_operator',
    schedule_interval='@daily',  # '@daily'==timedelta(days=1)
    # start_date=datetime(2022, 7, 22, 21)

) as dag:
    # define the tasks
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={
            # 'name': 'Jake',
            'age': 25
        }
    )
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name_func
    )

    # task pipeline
    task2 >> task1
    