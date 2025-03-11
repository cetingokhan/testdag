from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.providers.cncf.kubernetes.operators.job import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('sample_100_tasks_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    start = DummyOperator(task_id='start')
    
    tasks = []
    for i in range(1, 101):
        task = KubernetesPodOperator(
                task_id=f'task_{i}',
                name=f'task_{i}',
                namespace='airflow',
                image='python:3.8-slim',
                cmds=["python", "-c"],
                arguments=["print('Hello from the Python base image!')"],
                get_logs=True,
                queue='kubernetes'
            )


        #task = DummyOperator(task_id=f'task_{i}')
        tasks.append(task)
    
    end = DummyOperator(task_id='end')
    
    start >> tasks >> end
