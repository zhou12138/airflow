
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator

# 定义 DAG
with DAG(
    dag_id='demo_hello_world',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    # 定义任务
    hello_task = BashOperator(
        task_id='say_hello_yes',
        bash_command='echo "Hello, Airflow!"'
    )

    hello_task2 = BashOperator(
        task_id='say_hello_yes1',
        bash_command='echo "Hello, Airflow!"'
    )

    run_adf_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_my_adf_pipeline",
        pipeline_name="AirflowtriggerTest",
        resource_group_name="c2s",
        factory_name="ADFCMReplayV3",
        azure_data_factory_conn_id="azure_data_explorer_default",
        wait_for_termination=True,  # 是否等待 pipeline 执行完成
        deferrable=True  # 可选：使用 deferrable 模式释放 worker slot
    )

    # 设置任务依赖关系
    hello_task >> hello_task2 >> run_adf_pipeline
