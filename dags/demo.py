
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# 定义 DAG
with DAG(
    dag_id='demo_hello_world',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    # 定义一个任务
     hello_task = BashOperator(
        task_id='say_hello_yes',
        bash_command='echo "Hello, Airflow!"')

     hello_task2 = BashOperator(
        task_id='say_hello_yes1',
        bash_command='echo "Hello, Airflow!"')
    hello_task=>hello_task2

# 定义 DAG
with DAG(
    dag_id='demo_hello_world2',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    # 定义一个任务
     hello_task = BashOperator(
        task_id='say_hello_yes1',
        bash_command='echo "Hello, Airflow!"')


