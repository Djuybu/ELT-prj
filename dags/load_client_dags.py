from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Định nghĩa DAG
dag = DAG(
    'load_client_csv_to_delta',  # Tên DAG
    description='DAG to load client data to Delta Lake using Spark',
    schedule_interval='@daily',  # Chạy hàng ngày
    start_date=datetime(2025, 4, 12),
    catchup=False
)

# Định nghĩa lệnh bash để chạy spark-submit
spark_submit_command = """
spark-submit \
  --master local[*] \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.jars=/opt/spark/jar/delta-spark_2.13-3.3.0.jar \
  --py-files /home/djuybu/Desktop/BigData_BigProject/src/data_loaders/transactions/csv/load_client_csv.py
"""

# Sử dụng BashOperator để chạy lệnh spark-submit
load_to_delta_task = BashOperator(
    task_id='run_spark_submit',
    bash_command=spark_submit_command,
    dag=dag
)

