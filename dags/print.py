
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

def list_blobs():
    # 使用托管身份自动获取凭据
    credential = DefaultAzureCredential()

    # 替换为你的存储账户名
    account_url = "https://c2sfunctionappv2test.blob.core.windows.net"
    container_name = "airflow-dags"

    # 初始化 BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)

    # 列出 blob 文件
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print(f"Blob name: {blob.name}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1
}

with DAG(
    dag_id='list_azure_blob_files',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['azure', 'blob', 'managed-identity']
) as dag:

    list_blob_task = PythonOperator(
        task_id='list_blob_files',
        python_callable=list_blobs
    )

    list_blob_task
