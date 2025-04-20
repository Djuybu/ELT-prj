from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='clients_etl_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',  # chạy mỗi tuần
    catchup=False,
    description='ETL pipeline from CSV to bronze to silver layer',
    tags=['delta', 'bronze', 'silver']
) as dag:

    # 1️⃣ Task: Load raw CSV lên bronze
    load_to_bronze = BashOperator(
        task_id='load_csv_to_bronze',
        bash_command='spark-submit /opt/airflow/scripts/update_raw_csv_client_to_bronze.py'
    )

    # 2️⃣ Task: Transform từ bronze sang silver
    transform_to_silver = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command='spark-submit /opt/airflow/scripts/transform_bronze_clients_to_silver.py'
    )

    load_to_bronze >> transform_to_silver
