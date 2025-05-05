from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email': ['data_alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

# Define the DAG
dag = DAG(
    'kafka_to_gcs_simple_pipeline',
    default_args=default_args,
    description='Simple pipeline to consume products from Kafka and store them to GCS using spark-submit',
    schedule_interval='0 */4 * * *',  # Run every 4 hours
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['kafka', 'spark', 'products', 'streaming', 'gcs'],
)

# Define path for the PySpark script
SPARK_SCRIPT_PATH = Variable.get("SPARK_SCRIPT_PATH", "/opt/airflow/jobs/upload_to_bronze/kafka_to_gcs_processor.py")
# Check if script exists
check_script = BashOperator(
    task_id='check_script_exists',
    bash_command=f'test -f {SPARK_SCRIPT_PATH} || (echo "PySpark script not found at {SPARK_SCRIPT_PATH}" && exit 1)',
    dag=dag,
)

# Run the Spark job using spark-submit
run_spark_job = BashOperator(
    task_id='run_spark_kafka_to_gcs',
    bash_command=f'''
    spark-submit \
      --master local[*] \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0 \
      --conf "spark.executor.cores=2" \
      --conf "spark.executor.memory=4g" \
      --conf "spark.driver.memory=2g" \
      --conf "spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0" \
      --conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
      --conf "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" \
      --conf "spark.sql.shuffle.partitions=4" \
      {SPARK_SCRIPT_PATH} \
      --kafka_brokers "localhost:9092" \
      --kafka_topic "new_product" \
      --gcs_path "gs://bigdata-team3-uet-zz/bronze/products" \
      --checkpoint_path "gs://bigdata-team3-uet-zz/checkpoints/products" \
      --timeout_minutes 30
    ''',
    dag=dag,
)

# Verify data was written to GCS
verify_data = BashOperator(
    task_id='verify_data_written',
    bash_command='''
    TODAY=$(date +%Y-%m-%d)
    gsutil ls -l gs://bigdata-team3-uet-zz/bronze/products/first_issue_date=$TODAY/ || \
    (echo "No data found for today in GCS" && exit 1)
    ''',
    dag=dag,
)

# Define a task to mark completion
complete = DummyOperator(
    task_id='complete',
    dag=dag,
)

# Define the workflow
check_script >> run_spark_job >> verify_data >> complete
