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
    dag_id='debug_insta_data',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    description='Debugging Instagram data transformation',
    tags=['delta', 'bronze', 'silver']
) as dag:

    

  transform_insta_to_silver = BashOperator(
        task_id='transform_insta_to_silver',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar \
        /opt/airflow/jobs/transform_to_silver/social_media/transform_instagram_to_silver.py
        """
        )