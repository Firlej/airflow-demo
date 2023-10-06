from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'oskar',
    'retries': 5,
    'retries_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='bash_dag_catchup_backfill_v1',
    default_args=default_args,
    description='bla bla bla my first dag',
    start_date=datetime(2023, 9, 15, 10),
    schedule='@daily',
    catchup=False,
) as dag:
    
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world',
    )

    task1

# sudo docker exec -it 9281b0771cde bash

# airflow dags backfill -s 2023-09-01 -e 2021-11-30

# airflow dags backfill bash_dag_catchup_backfill_v1 -s 2023-09-01 -e 2021-11-30