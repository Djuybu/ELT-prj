
from pyspark.sql import SparkSession
from delta.tables import DeltaTable


spark = SparkSession.builder \
    .appName("MergeClientsSilverToGold") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.3.1.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


import random
from pyspark.sql.functions import col, trunc, year, month, dayofmonth, lit, expr


# Đường dẫn
silver_purchase_path = "gs://bigdata-team3-uet-zz/silver/purchases"
gold_fact_sales_path = "gs://bigdata-team3-uet-zz/gold/fact_sales"

# Đọc dữ liệu từ silver
purchases_df = spark.read.format("delta").load(silver_purchase_path).select(
    "product_id",
    "client_id",
    "transaction_id",
    "transaction_datetime",
    "regular_points_received",
    "purchase_sum",
    "store_id",
    "product_quantity"
)

# Xử lý dữ liệu: tính các cột mới
from pyspark.sql.functions import rand

purchases_processed = purchases_df.withColumn(
    "discount_amount", lit(0.0)
).withColumn(
    "unit_price",
    (col("purchase_sum") / col("product_quantity")).alias("unit_price")
).withColumn(
    "date_key",
    expr("date_format(transaction_datetime, 'yyyyMMdd')").cast("int")
).withColumn(
    "transaction_datetime_date",
    col("transaction_datetime").cast("date")
).withColumn(
    "regular_points_received", (rand() * 100).cast("double")  # Gán ngẫu nhiên từ 0 đến 100
).select(
    "product_id",
    "client_id",
    "transaction_id",
    "transaction_datetime_date",
    "date_key",
    "product_quantity",
    "unit_price",
    "discount_amount",
    "purchase_sum",
    "regular_points_received",
    "store_id"
)

# Kiểm tra bảng fact_sales đã tồn tại hay chưa
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, gold_fact_sales_path):
    fact_sales = DeltaTable.forPath(spark, gold_fact_sales_path)

    # Merge dữ liệu
    fact_sales.alias("target").merge(
        purchases_processed.alias("source"),
        "target.transaction_id = source.transaction_id"
    ).whenMatchedUpdate(
        set={
            "product_id": "source.product_id",
            "client_id": "source.client_id",
            "store_id": "source.store_id",
            "date_key": "source.date_key",
            "transaction_datetime": "source.transaction_datetime_date",
            "product_quantity": "source.product_quantity",
            "unit_price": "source.unit_price",
            "discount_amount": "source.discount_amount",
            "purchase_sum": "source.purchase_sum",
            "regular_points_received": "source.regular_points_received"
        }
    ).whenNotMatchedInsert(
        values={
            "transaction_id": "source.transaction_id",
            "product_id": "source.product_id",
            "client_id": "source.client_id",
            "store_id": "source.store_id",
            "date_key": "source.date_key",
            "transaction_datetime": "source.transaction_datetime_date",
            "product_quantity": "source.product_quantity",
            "unit_price": "source.unit_price",
            "discount_amount": "source.discount_amount",
            "purchase_sum": "source.purchase_sum",
            "regular_points_received": "source.regular_points_received"
        }
    ).execute()
else:
    # Nếu bảng chưa tồn tại, tạo mới
    purchases_processed.write.format("delta").save(gold_fact_sales_path)



print("✅ Merge purchase vào fact_sales đã hoàn tất.")