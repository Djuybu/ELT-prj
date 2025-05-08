import pandas as pd
import random
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, lit, to_date, expr
)
from delta.tables import DeltaTable

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Update dim_time") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.3.1.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đường dẫn tới bảng dim_time
dim_time_path = "gs://bigdata-team3-uet-zz/gold/dim_times"

# Bước 1: Đọc bảng dim_time và lấy ngày mới nhất đã có
dim_time_df = spark.read.format("delta").load(dim_time_path)
max_date_row = dim_time_df.agg({"full_date": "max"}).collect()[0]
max_date = max_date_row["max(full_date)"]

if max_date is None:
    # Nếu chưa có dữ liệu, bắt đầu từ ngày hôm nay hoặc tùy ý
    max_date = date.today()

start_date = max_date + timedelta(days=1)
end_date = date.today()

# Nếu không có ngày mới cần thêm
if start_date > end_date:
    print("Bảng đã cập nhật đến ngày 오늘")
else:
    # Bước 2: Tạo danh sách ngày từ start_date đến end_date
    dates = pd.date_range(start=start_date, end=end_date)
    dates_df = pd.DataFrame({"full_date": dates})

    # Bước 3: Tính các thuộc tính của từng ngày
    new_days_spark_df = spark.createDataFrame(dates_df)
    new_days_spark_df = new_days_spark_df.withColumn(
        "full_date", to_date(col("full_date"))
    ).withColumn(
        "date_key", expr("cast(date_format(full_date, 'yyyyMMdd') as int)")
    ).withColumn(
        "day_of_week", date_format(col("full_date"), "EEEE")
    ).withColumn(
        "day_number_of_week", dayofweek(col("full_date"))
    ).withColumn(
        "day_of_month", dayofmonth(col("full_date"))
    ).withColumn(
        "day_of_year", dayofyear(col("full_date"))
    ).withColumn(
        "week_of_year", weekofyear(col("full_date"))
    ).withColumn(
        "month", month(col("full_date"))
    ).withColumn(
        "month_name", date_format(col("full_date"), "MMMM")
    ).withColumn(
        "quarter", quarter(col("full_date"))
    ).withColumn(
        "year", year(col("full_date"))
    ).withColumn(
        "is_weekend", (dayofweek(col("full_date")).isin([1, 7])).cast("int")
    ).withColumn(
        "is_holiday", lit(0)  # hoặc thay bằng danh sách các ngày lễ phù hợp
    )

    # Bước 4: Tải bảng dim_time và thêm các ngày mới vào
    dim_time_table = DeltaTable.forPath(spark, dim_time_path)

    # Lấy các ngày đã tồn tại để tránh trùng lặp
    existing_dates_df = dim_time_table.toDF().select("full_date")

    # Chỉ giữ các ngày mới chưa có trong bảng
    new_dates_to_insert = new_days_spark_df.join(
        existing_dates_df,
        on="full_date",
        how="left_anti"
    )

    # Bước 5: Thêm các ngày mới vào bảng dim_time
    if new_dates_to_insert.count() > 0:
        new_dates_to_insert.write.format("delta").mode("append").save(dim_time_path)
        print(f"Đã cập nhật bảng dim_time với {new_dates_to_insert.count()} ngày mới.")
    else:
        print("Không có ngày mới để cập nhật.")

# Dừng Spark session (tùy chọn)
spark.stop()