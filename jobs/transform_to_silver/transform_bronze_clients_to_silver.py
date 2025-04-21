from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from py4j.java_gateway import java_import

# Khởi tạo SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession.builder \
    .appName("MergeBronzeToSilver") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.3.1.jar,/opt/spark/jars/gcs-connector-hadoop3-2.2.4-shaded.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "false") \
    .getOrCreate()

# Đường dẫn input/output
bronze_path = "gs://bigdata-team3-uet-zz/bronze/clients"
silver_path = "gs://bigdata-team3-uet-zz/silver/clients"
primary_key = "client_id"

# Đọc dữ liệu từ bronze (trong 7 ngày gần nhất)
bronze_df = spark.read.format("delta").load(bronze_path) \
    .filter("ingestion_time >= current_timestamp() - INTERVAL 7 DAYS")

# Lấy bản ghi mới nhất cho mỗi client
window_spec = Window.partitionBy(primary_key).orderBy(col("ingestion_time").desc())
latest_df = bronze_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter("row_num = 1") \
    .drop("row_num", "source_file", "ingestion_time")

# Kiểm tra thư mục Delta table đã tồn tại chưa
java_import(spark._jvm, "org.apache.hadoop.fs.Path")
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

if fs.exists(spark._jvm.Path(silver_path)):
    # Nếu đã tồn tại bảng silver, thực hiện merge
    silver_table = DeltaTable.forPath(spark, silver_path)

    silver_table.alias("target").merge(
        latest_df.alias("source"),
        f"target.{primary_key} = source.{primary_key}"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    # Nếu chưa có, ghi dữ liệu mới tạo bảng silver
    latest_df.write.format("delta").save(silver_path)

print("✅ Merge from bronze to silver completed.")
