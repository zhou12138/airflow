
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# 定义 DAG
with DAG(
    dag_id='simple_hello_world',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    # 定义一个任务
     hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello, Airflow!"')
