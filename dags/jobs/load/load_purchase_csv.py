from datetime import datetime
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

sc = SparkContext.getOrCreate()
spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config('spark.jars', '/opt/spark/jars/delta-spark_2.12-3.3.1.jar') \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://10.128.0.2:9000") \
    .config("spark.hadoop.hadoop.home.dir", "/opt/hadoop")  \
    .config("spark.hadoop.yarn.resourcemanager.address", "hdfs://10.128.0.2:8032")\
    .getOrCreate()

# create dataframe from csv file
def load_purchase_csv(file_path: str) -> DataFrame:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.show()
    return df

def check_data(df: DataFrame) -> bool:
    # Get the row with the latest "transaction_datetime"
    latest_date_row = df.orderBy(col("transaction_datetime").desc()).first()
    latest_date = latest_date_row["transaction_datetime"]

    control_file = "/opt/airflow/files/control.json"
    with open(control_file, "r") as f:
        control_data = json.load(f)
        last_updated_str = control_data["last_updated"]["purchases"]
        last_updated = datetime.strptime(last_updated_str, '%Y-%m-%dT%H:%M:%SZ')

    # Check and update if newer data is found
    if latest_date > last_updated:
        print("Newer data found. Updating control file.")
        control_data["last_updated"]["purchases"] = latest_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        with open(control_file, "w") as f:
            json.dump(control_data, f, indent=4)
        return True
    else:
        print("Data is not up to date.")
        return False
    
def load_to_delta(df: DataFrame) -> None:
    # save delta table to Hadoop HDFS
    df.write.format("delta").mode("overwrite").save("hdfs://10.128.0.2:9000/delta/purchase")  # Thay đổi IP cho phù hợp

if __name__ == "__main__":
    # Example usage
    file_path = "/opt/airflow/files/transactions/purchases.csv"
    df = load_purchase_csv(file_path)
    if check_data(df):
        load_to_delta(df)
