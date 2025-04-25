import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, sequence, to_date, year, month, dayofmonth, dayofweek, weekofyear, dayofyear, quarter, date_format
from pyspark.sql.types import DateType

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
purchases_df = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/delta/purchases")
stores_df = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/delta/store")

clients_warehouse = clients_df
products_warehouse = products_df
stores_warehouse = stores_df

# initialize dim_time
# Khoảng thời gian muốn tạo
start_date = "2018-11-22"
end_date = "2019-03-19"

# Tạo dãy ngày liên tiếp
df_date = spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_seq") \
    .selectExpr("explode(date_seq) as full_date")

# Tạo các cột dim_time
dim_time = df_date.withColumn("date_key", expr("CAST(date_format(full_date, 'yyyyMMdd') AS INT)")) \
    .withColumn("day_of_week", date_format(col("full_date"), "EEEE")) \
    .withColumn("day_number_of_week", dayofweek("full_date")) \
    .withColumn("day_of_month", dayofmonth("full_date")) \
    .withColumn("day_of_year", dayofyear("full_date")) \
    .withColumn("week_of_year", weekofyear("full_date")) \
    .withColumn("month", month("full_date")) \
    .withColumn("month_name", date_format("full_date", "MMMM")) \
    .withColumn("quarter", quarter("full_date")) \
    .withColumn("year", year("full_date")) \
    .withColumn("is_weekend", expr("CASE WHEN dayofweek(full_date) IN (1, 7) THEN 1 ELSE 0 END")) \
    .withColumn("is_holiday", expr("0"))  # Bạn có thể cập nhật logic holiday riêng nếu có

#initialize fact_sales
transactions = purchases_df \
    .withColumn("client_id", col("client_id").cast("string")) \
    .withColumn("store_id", col("store_id").cast("string")) \
    .withColumn("product_quantity", col("product_quantity").cast("int")) \
    .withColumn("transaction_datetime", to_date("transaction_datetime")) \
    .withColumn("date_key", expr("CAST(date_format(transaction_datetime, 'yyyyMMdd') AS INT)"))

# Chuẩn hóa bảng sản phẩm để có unit_price
products = products_df.withColumnRenamed("product_price", "unit_price")

# Join với products để lấy đơn giá
fact_sales = transactions.join(products.select("product_id", "unit_price"), on="product_id", how="left")

# Tính discount_amount = 5% đơn giá * số lượng
fact_sales = fact_sales.withColumn("discount_amount", expr("unit_price * product_quantity * 0.05"))

# Tính lại purchase_sum nếu chưa chuẩn
fact_sales = fact_sales.withColumn("purchase_sum", expr("unit_price * product_quantity - discount_amount"))

# Gán cố định payment_method nếu không có
fact_sales = fact_sales.withColumn("payment_method", expr("'POS'"))

# Chọn đúng thứ tự các trường theo mô hình fact_sales
fact_sales_final = fact_sales.select(
    "transaction_id",
    "product_id",   
    "client_id",
    "store_id",
    "date_key",
    "transaction_datetime",
    "product_quantity",
    "unit_price",
    "discount_amount",
    "purchase_sum",
    "regular_points_received",
    "payment_method"
)

# Ghi xuống định dạng Delta Lake
fact_sales_final.printSchema()
fact_sales_final.show(10)

fact_sales_final.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/delta/warehouse/fact_sales")
dim_time.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/delta/warehouse/dim_times")
clients_warehouse.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/delta/warehouse/clients")
products_warehouse.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/delta/warehouse/products")
stores_warehouse.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/delta/warehouse/stores")
