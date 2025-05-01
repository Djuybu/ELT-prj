from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Định nghĩa DAG
dag = DAG(
    'load_social_csv_to_bronze',
    description='DAG to load social media data to Bronze layer using Spark and Delta Lake',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 12),
    catchup=False
)

# Câu lệnh spark-submit để chạy job
spark_submit_command = """
 /opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar \
  /opt/airflow/docker/ELT-prj/load_social_media_to_bronze.py
"""

# Tạo task sử dụng BashOperator
load_to_bronze_task = BashOperator(
    task_id='run_social_bronze_spark_job',
    bash_command=spark_submit_command,
    dag=dag
)
