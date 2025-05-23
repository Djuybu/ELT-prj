from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Định nghĩa DAG
dag = DAG(
    'load_social_data_to_silver',  # Tên DAG
    description='DAG to load social media data to silver layer of Delta Lake using Spark',
    schedule_interval='@daily',  # Chạy hàng ngày
    start_date=datetime(2025, 4, 12),
    catchup=False
)

# Định nghĩa lệnh bash để chạy spark-submit
facebook_transform_command = """
 /opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar \
  /opt/airflow/jobs/transform_to_silver/social_media/transform_facebook_to_silver.py
"""

instagram_transform_command = """
 /opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar \
  /opt/airflow/jobs/transform_to_silver/social_media/transform_instagram_to_silver.py
"""

# Sử dụng BashOperator để chạy lệnh spark-submit
transform_facebook_task = BashOperator(
    task_id='transform_facebook',
    bash_command=facebook_transform_command,
    dag=dag
)

transform_instagram_task = BashOperator(
    task_id='transform_instagram',
    bash_command=instagram_transform_command,
    dag=dag
)



