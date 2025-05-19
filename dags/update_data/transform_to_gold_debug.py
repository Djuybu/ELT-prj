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
    dag_id='social_elt_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    description='ETL pipeline from CSV to bronze to silver layer',
    tags=['delta', 'bronze', 'silver']
) as dag:

    transform_all_to_gold = BashOperator(
        task_id='transform_all_to_gold',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar \
        /opt/airflow/jobs/transform_to_gold/transform_social_media_to_gold.py
        """
    )

    transform_all_to_gold