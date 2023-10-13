# my_dag.py
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 12),
}

with DAG('variables_test',
         default_args=default_args,
         schedule_interval='@daily',
         ) as dag:

    task = BashOperator(
        task_id='bash_task',
        bash_command='echo $MY_VAR',
        env={'MY_VAR': Variable.get("my_var")},  # Pass environment variable to Bash
    )
