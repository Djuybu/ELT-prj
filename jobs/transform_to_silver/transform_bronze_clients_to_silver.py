from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number, coalesce
from pyspark.sql.window import Window
import os

# ============================
# ⚙️ Spark Session Config
# ============================
sc = SparkContext.getOrCreate()
spark = SparkSession.builder \
    .appName("MergeBronzeToSilver") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-2.2.4-shaded.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "false") \
    .getOrCreate()

bronze_path = "gs://bigdata-team3-uet-zz/bronze/clients"
silver_path = "gs://bigdata-team3-uet-zz/silver/clients"
primary_key = "client_id"

# ============================
# 📥 Load latest data from Bronze
# ============================
bronze_df = spark.read.format("parquet").load(bronze_path) \
    .filter("ingestion_time >= current_timestamp() - INTERVAL 7 DAYS")

window_spec = Window.partitionBy(primary_key).orderBy(col("ingestion_time").desc())
latest_df = bronze_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter("row_num = 1") \
    .drop("row_num", "source_file", "ingestion_time")

# ============================
# 🔁 Merge to Silver (if exists)
# ============================
def silver_exists(path):
    try:
        spark.read.parquet(path)
        return True
    except:
        return False

if silver_exists(silver_path):
    silver_df = spark.read.parquet(silver_path)

    merged_df = latest_df.alias("source").join(
        silver_df.alias("target"),
        on=primary_key,
        how="outer"
    )

    for col_name in latest_df.columns:
        if col_name != primary_key:
            merged_df = merged_df.withColumn(
                col_name,
                coalesce(f"source.{col_name}", f"target.{col_name}")
            )

    merged_df.select(latest_df.columns).write.mode("overwrite").parquet(silver_path)
else:
    latest_df.write.mode("overwrite").parquet(silver_path)

print("✅ Merge from bronze to silver completed.")
