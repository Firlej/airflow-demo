from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.models import TaskInstance

default_args = {
    'owner': 'oskar',
    'retries': 5,
    'retries_delay': timedelta(minutes=2)
}

###

def get_name():
    return "Jamal"

def get_full_name(ti):
    ti.xcom_push(key='first_name', value='Jimbo')
    ti.xcom_push(key='last_name', value='White')

###

def get_age(ti):
    ti.xcom_push(key='age', value=69*2)

###

def greet_simple(age, ti: TaskInstance):
    name = ti.xcom_pull(task_ids='get_name')
    print(f"hello {name=} {age=}")

def greet_full(age, ti: TaskInstance):
    first_name = ti.xcom_pull(task_ids='get_full_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_full_name', key='last_name')
    print(f"hello {first_name=} {last_name=} {age=}")

def greet_with_pulled_age(ti: TaskInstance):
    first_name = ti.xcom_pull(task_ids='get_full_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_full_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"hello {first_name=} {last_name=} {age=}")

###

with DAG(
    dag_id='python_dag_v8',
    default_args=default_args,
    description='bla bla bla my python dag',
    start_date=datetime(2023, 9, 15, 10),
    schedule_interval='@daily',
) as dag:
    
    get_age = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
        op_kwargs={}
    )
    
    get_name = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
        op_kwargs={}
    )
    
    get_full_name = PythonOperator(
        task_id='get_full_name',
        python_callable=get_full_name,
        op_kwargs={}
    )

    ###
    
    greet_simple = PythonOperator(
        task_id='greet_simple',
        python_callable=greet_simple,
        op_kwargs={
            # "name": "oskar",
            "age": 69
        }
    )
    
    greet_full = PythonOperator(
        task_id='greet_full',
        python_callable=greet_full,
        op_kwargs={
            # "name": "oskar",
            "age": 69
        }
    )
    
    greet_with_pulled_age = PythonOperator(
        task_id='greet_with_pulled_age',
        python_callable=greet_with_pulled_age,
        op_kwargs={
            # "name": "oskar",
            # "age": 69
        }
    )

    # task1 >> task2

    get_name >> greet_simple
    get_full_name >> [greet_full]
    [get_full_name, get_age] >> greet_with_pulled_age