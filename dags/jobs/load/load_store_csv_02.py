from datetime import datetime
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

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
def load_store_csv(file_path: str) -> DataFrame:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.withColumn("first_issue_date", current_timestamp())
    df.show()
    return df

def check_data(df: DataFrame) -> bool:
    latest_date_row = df.orderBy(col("first_issue_date").desc()).first()
    latest_date = latest_date_row["first_issue_date"]

    control_file = "/opt/airflow/files/control.json"
    with open(control_file, "r") as f:
        control_data = json.load(f)
        last_updated_str = control_data["last_updated"]["stores"]
        last_updated = datetime.strptime(last_updated_str, '%Y-%m-%dT%H:%M:%SZ')

    if latest_date > last_updated:
        print("Newer data found. Updating control file.")
        # Update the control file with the new date
        control_data["last_updated"]["stores"] = latest_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        with open(control_file, "w") as f:
            json.dump(control_data, f, indent=4)
        return True
    else:
        print("Data is not up to date.")
        return False

    
def load_to_delta(df: DataFrame) -> None:
    df.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/delta/store")

if __name__ == "__main__":
    # Example usage
    file_path = "/opt/airflow/files/transactions/stores.csv"
    df = load_store_csv(file_path)
    if check_data(df):
        load_to_delta(df)
