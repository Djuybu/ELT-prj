from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, trim, lower, current_timestamp, monotonically_increasing_id, min as spark_min
from pyspark.sql.types import StringType, LongType
from pyspark.sql.functions import udf
import os
import random
from datetime import datetime, timedelta

# ✳️ Giả lập nội dung post
sample_reviews = [
    "This product changed my life!", "Highly recommended!", "Great value for money!",
    "Would buy again.", "Not what I expected.", "Perfect gift idea!"
]

# ⚙️ Spark Session
spark = SparkSession.builder \
    .appName("NormalizeSocialMedia") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "/opt/airflow/files"

# Liệt kê tất cả các file trong thư mục
platform_files = [f for f in os.listdir(base_path) if f.endswith('.csv')]

# Tổng hợp
users_df_all, posts_df_all, hashtags_df_all, post_hashtag_df_all, user_mentions_df_all = None, None, None, None, None
user_id_counter = 0
hashtag_id_counter = 0
user_map = {}
hashtag_map = {}

# UDF sinh ngẫu nhiên
random_review_udf = udf(lambda: random.choice(sample_reviews), StringType())
random_int_udf = udf(lambda: random.randint(100, 100000), LongType())

# Đọc từng file
for filename in platform_files:
    platform = filename.split("-")[0]  # Giả sử tên platform có dạng "Facebook-datasets.csv"
    df = spark.read.option("header", True).csv(os.path.join(base_path, filename))
    df_cols = df.columns

    # USERS
    if "user_posted" in df_cols:
        user_urls = df.select("user_posted").dropna().distinct().withColumnRenamed("user_posted", "user_url")
        
        # Tạo user_id và các thông tin khác
        users_df = user_urls.withColumn("user_id", monotonically_increasing_id() + user_id_counter) \
            .withColumn("display_name", split(col("user_url"), "/").getItem(-1)) \
            .withColumn("bio", lit(f"User from {platform}")) \
            .withColumn("is_verified", lit(True)) \
            .withColumn("followers_count", random_int_udf()) \
            .withColumn("following_count", random_int_udf()) \
            .withColumn("post_count", random_int_udf()) \
            .withColumn("type_of_account", lit(platform)) \
            .select("user_id", "display_name", "bio", "is_verified", "followers_count", "following_count", "post_count", "type_of_account") \
            .dropDuplicates(["display_name"])
        
        user_id_counter += 100000
        new_user_map = users_df.select("display_name", "user_id").rdd.collectAsMap()
        user_map.update(new_user_map)

        users_df_all = users_df if users_df_all is None else users_df_all.union(users_df)

    # POSTS
    if "post_url" in df_cols and "user_posted" in df_cols:
        # Trích xuất post_id từ post_url
        extract_post_id_udf = udf(lambda url: url.rstrip("/").split("/")[-1] if url else None, StringType())
        get_user_id_udf = udf(lambda url: user_map.get(url.split("/")[-1]), LongType())
        
        # Lấy comment_date và trừ đi 1 ngày
        if "comment_date" in df_cols:
            try:
                min_date_row = df.select(spark_min("comment_date").alias("min_date")).collect()[0]
                min_date_str = min_date_row["min_date"]
                min_date = datetime.strptime(min_date_str, "%Y-%m-%d") - timedelta(days=1)
            except:
                min_date = datetime.now() - timedelta(days=1)
        else:
            min_date = datetime.now() - timedelta(days=1)

        # Tạo bảng posts_df
        posts_df = df.withColumn("post_id", extract_post_id_udf(col("post_url"))) \
            .withColumn("user_id", get_user_id_udf(col("user_posted"))) \
            .withColumn("post_content", random_review_udf()) \
            .withColumn("post_date", lit(min_date.strftime("%Y-%m-%d"))) \
            .withColumn("post_url", col("post_url")) \
            .select("post_id", "user_id", "post_content", "post_date", "post_url") \
            .dropDuplicates(["post_id"])

        posts_df_all = posts_df if posts_df_all is None else posts_df_all.union(posts_df)

# 🟫 Ghi xuống Bronze layer
if posts_df_all:
    posts_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite") \
        .save("gs://bigdata-team3-uet-zz/bronze/dim_posts")

print("✅ DONE: Normalized and saved to Bronze layer.")
