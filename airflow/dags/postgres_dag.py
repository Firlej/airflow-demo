from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

from airflow.providers import http

# pip install 'apache-airflow[postgres]'
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'oskar',
    'retries': 5,
    'retries_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='postgres_dag_v1',
    default_args=default_args,
    description='bla bla bla my first postgres dag',
    start_date=datetime(2023, 9, 15, 10),
    schedule_interval='0 0 * * *',
) as dag:
    
    task1 = PostgresOperator(
        task_id='first_task',
        postgres_conn_id='airflow_postgres',
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id,
                primary key (dt, dag_id)
            )
        """
    )
    
    task1