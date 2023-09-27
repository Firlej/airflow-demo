from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.models import TaskInstance

from airflow.decorators import dag, task

default_args = {
    'owner': 'oskar',
    'retries': 5,
    'retries_delay': timedelta(minutes=2)
}

@dag(
    dag_id='python_taskflow_dag_v2',
    default_args=default_args,
    description='bla bla bla my python dag',
    start_date=datetime(2023, 9, 15, 10),
    schedule='20 4 * * Tue,Fri,Sun', # https://crontab.guru/#20_4_*_*_Tue,Fri,Sun
    # schedule='@hourly',
)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Jamal",
            "last_name": "White"
        }

    @task()
    def get_age():
        return 69

    @task()
    def greet(first_last_name, age):
        print(f"hello {first_last_name=} {age=}")


    first_last_name = get_name()
    age = get_age()

    greet(first_last_name, age)

dag = hello_world_etl()




