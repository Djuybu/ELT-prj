import findspark
import pandas as pd
import random
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession

findspark.init()

# Khởi tạo SparkContext và SparkSession
sparkContext = SparkContext.getOrCreate()
spark = SparkSession(sparkContext)

# Đọc dữ liệu từ file JSON
with open("new.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Kiểm tra cấu trúc của 'data' để chắc chắn
print("Data structure:", data)

# Đọc dữ liệu từ CSV bằng PySpark
df_product = spark.read.csv("products.csv", header=True, inferSchema=True)

# Kiểm tra schema của DataFrame
df_product.printSchema()

# Loại bỏ các cột không cần thiết
df_product = df_product.drop("netto", "is_own_trademark", "is_alcohol", "segment_id", "vendor_id", "brand_id")

# Chuyển DataFrame PySpark sang Pandas DataFrame
df_product_pd = df_product.toPandas()

# Kiểm tra cấu trúc của data và đảm bảo nó là danh sách các từ điển
if isinstance(data, list) and isinstance(data[0], dict):
    # Mỗi cột sẽ lấy một sản phẩm ngẫu nhiên khác nhau
    df_product_pd['brand_name'] = [random.choice(data)['brand_name'] for _ in range(len(df_product_pd))]
    df_product_pd['product_name'] = [random.choice(data)['product_name'] for _ in range(len(df_product_pd))]
    df_product_pd['product_price'] = [random.choice(data)['price'] for _ in range(len(df_product_pd))]
    df_product_pd['product_category'] = [random.choice(data)['category'] for _ in range(len(df_product_pd))]
else:
    print("Cấu trúc dữ liệu không hợp lệ. Vui lòng kiểm tra lại file JSON.")

# Chuyển lại từ Pandas DataFrame thành PySpark DataFrame
df_product = spark.createDataFrame(df_product_pd)

# In schema và một vài dòng dữ liệu để kiểm tra
df_product.printSchema()
df_product.show(5, truncate=False)

df_product.coalesce(1).write.csv("product_cleaned", header=True, mode="overwrite")
