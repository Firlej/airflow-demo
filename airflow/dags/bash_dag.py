from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'oskar',
    'retries': 5,
    'retries_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='bash_dag_v7',
    default_args=default_args,
    description='bla bla bla my first dag',
    start_date=datetime(2023, 9, 15, 10),
    schedule_interval='@daily',
) as dag:
    
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world',
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command='date',
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo XDXDXDXDX',
    )

    task4 = BashOperator(
        task_id='fourth_task',
        bash_command='echo 44444',
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # task1 >> task2
    # task1 >> task3

    task1 >> [task2, task3] >> task4
    task1 >> task4