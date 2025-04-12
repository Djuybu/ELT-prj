from datetime import datetime
import findspark
import json
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

sc = SparkContext.getOrCreate()
spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config('spark.jars', '/opt/spark/jar/delta-spark_2.13-3.3.0.jar') \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# create dataframe from csv file
def load_client_csv(file_path: str) -> DataFrame:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.show()
    return df

def check_data(df: DataFrame) -> bool:
    # get the row with the latest "first_issue_date"
    latest_date_row = df.orderBy(df["first_issue_date"].desc()).first()
    with open("../files/control.json") as f:
        last_updated = json.load(f)["last_updated"]["clients"]
        last_updated = datetime.strptime(last_updated, '%Y-%m-%dT%H:%M:%SZ')
    # check if the latest date is greater than the last updated date
    if latest_date_row["first_issue_date"] > last_updated:
        print("Data is up to date")
        return True
    else:
        print("Data is not up to date")
        return False
    
def load_to_delta(df: DataFrame) -> None:
    # save delta table to localhost hdfs
    df.write.format("delta").mode("overwrite").save("hdfs://localhost:9000/delta/client")

if __name__ == "__main__":
    # Example usage
    file_path = "../files/clients.csv"
    df = load_client_csv(file_path)
    if check_data(df):
        load_to_delta(df)