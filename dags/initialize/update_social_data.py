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
    dag_id='social_data_etl_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    description='ETL pipeline from raw CSV to bronze and silver layers for social media data',
    tags=['social', 'bronze', 'silver']
) as dag:

    # 1️⃣ Load raw CSV lên Bronze layer
    load_to_silver = BashOperator(
        task_id='load_csv_to_bronze',
        bash_command="""  
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --jars /opt/spark/jars/delta-spark_2.12-3.1.0.jar,/opt/spark/jars/delta-storage-2.4.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar \
        /opt/airflow/jobs/upload_to_bronze/process_social_media_to_bronze.py
        """
    )

    # 2️⃣ Transform từ Bronze sang Silver
    transform_to_gold = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command="""  
        pip install delta-spark==1.0.0 && \
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --jars /opt/spark/jars/delta-spark_2.12-3.1.0.jar,/opt/spark/jars/delta-storage-2.4.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar \
        /opt/airflow/jobs/transform_to_silver/transform_bronze_social_to_silver.py
        """
    )

    # Xác định thứ tự thực hiện các task: load_to_bronze -> transform_to_silver
    load_to_silver >> transform_to_gold
