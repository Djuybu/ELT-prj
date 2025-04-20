from datetime import datetime
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

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

# create dataframe from csv file
def load_purchase_csv(file_path: str) -> DataFrame:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.show()
    return df
    
def load_to_delta(df: DataFrame) -> None:
    df.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/delta/purchases")

if __name__ == "__main__":
    # Example usage
    file_path = "/opt/airflow/files/transactions/purchases.csv"
    df = load_purchase_csv(file_path)
    load_to_delta(df)
