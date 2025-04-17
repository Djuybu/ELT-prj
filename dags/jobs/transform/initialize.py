import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

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

clients_df = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/delta/clients")
products_df = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/delta/products")
transactions_df = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/delta/transactions")
stores_df = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/delta/stores")

