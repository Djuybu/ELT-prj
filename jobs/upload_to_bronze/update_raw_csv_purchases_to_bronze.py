from datetime import datetime
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name, current_timestamp

folder_path = "/opt/airflow/files/raw/purchases"

sc = SparkContext.getOrCreate()
spark = SparkSession.builder \
    .appName("DeltaLakeToGCS") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.3.1.jar,/opt/spark/jars/gcs-connector-hadoop3-2.2.4-shaded.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "false") \
    .getOrCreate()

def process_csv_file(file_path: str):
    print(f"Processing file: {file_path}")
    df = spark.read.option("header", True).csv(file_path)

    # Gắn thông tin metadata
    df = df.withColumn("ingestion_time", current_timestamp()) \
           .withColumn("source_file", input_file_name())

    # Append vào delta table
    df.write.format("delta") \
        .mode("append") \
        .save("gs://bigdata-team3-uet-zz/bronze/purchases")

if __name__ == "__main__":
    # Example usage
        # Lấy danh sách các file CSV
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            process_csv_file(file_path)

    print("✅ All files processed.")