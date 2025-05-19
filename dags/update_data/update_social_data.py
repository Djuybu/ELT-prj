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
        load_to_bronze = BashOperator(
        task_id='load_csv_to_bronze',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar \
        /opt/airflow/jobs/upload_to_bronze/upload_raw_socialmedia_csv.py
        """
        )
        transform_facebook_to_silver = BashOperator(
        task_id='transform_facebook_to_silver',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar \
        /opt/***/jobs/transform_to_silver/social_media/transform_facebook_to_silver.py
        """
        )
        transform_tiktok_to_silver = BashOperator(
        task_id='transform_tiktok_to_silver',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar \
        /opt/airflow/jobs/transform_to_silver/social_media/transform_tiktok_to_silver.py
        """
        )
        transform_instagram_to_silver = BashOperator(
        task_id='transform_instagram_to_silver',
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
        transform_twitter_to_silver = BashOperator(
        task_id='transform_twitter_to_silver',
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --jars /opt/spark/jars/delta-spark_2.13-3.3.0.jar,/opt/spark/jars/delta-storage-3.3.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar \
        /opt/airflow/jobs/transform_to_silver/social_media/transform_twitter_to_silver.py
        """
        )
        
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
        load_to_bronze >> [
            transform_facebook_to_silver,
            transform_tiktok_to_silver,
            transform_instagram_to_silver,
            transform_twitter_to_silver
        ] >> transform_all_to_gold
