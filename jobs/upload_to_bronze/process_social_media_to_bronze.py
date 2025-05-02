from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, trim, lower, current_timestamp, monotonically_increasing_id, min as spark_min
from pyspark.sql.types import StringType, LongType
from pyspark.sql.functions import udf
import os
import random

# ✳️ Random post content (giả lập nội dung)
sample_reviews = [
    "This product changed my life!", "Highly recommended!", "Great value for money!",
    "Would buy again.", "Not what I expected.", "Perfect gift idea!"
]

# ✳️ Danh sách các nguồn mạng xã hội
platform_files = {
    "Facebook": "Facebook-datasets.csv",
    "Instagram": "Instagram-datasets.csv",
    "Twitter": "Twitter-datasets.csv",
    "TikTok": "Tiktok-datasets.csv"
}

# ⚙️ Spark session
spark = SparkSession.builder \
    .appName("NormalizeSocialMedia") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "/opt/airflow/files"

# Các biến tổng hợp
users_df_all, posts_df_all, hashtags_df_all, post_hashtag_df_all, user_mentions_df_all = None, None, None, None, None
user_id_counter = 0
hashtag_id_counter = 0
user_posted_to_id = {}
hashtag_text_to_id = {}

# UDF sinh nội dung ngẫu nhiên
random_review_udf = udf(lambda: random.choice(sample_reviews), StringType())
random_int_udf = udf(lambda: random.randint(100, 100000), LongType())

for platform, filename in platform_files.items():
    df = spark.read.option("header", True).csv(os.path.join(base_path, filename))

    # USERS
    user_urls = df.select("user_posted").dropna().distinct().withColumnRenamed("user_posted", "user_posted_clean")

    users_df = user_urls.withColumn("user_id", monotonically_increasing_id() + user_id_counter) \
        .withColumn("display_name", split(col("user_posted_clean"), "/").getItem(-1)) \
        .withColumn("bio", col("user_posted_clean")) \
        .withColumn("is_verified", lit(True)) \
        .withColumn("followers_count", random_int_udf()) \
        .withColumn("following_count", random_int_udf()) \
        .withColumn("post_count", random_int_udf()) \
        .withColumn("type_of_account", lit(platform)) \
        .select("user_id", "display_name", "bio", "is_verified", "followers_count", "following_count", "post_count", "type_of_account")

    user_id_counter += 1000000
    user_map = users_df.select("bio", "user_id").rdd.collectAsMap()
    user_posted_to_id.update(user_map)

    # POSTS
    get_user_id_udf = udf(lambda url: user_posted_to_id.get(url), LongType())

    # Lấy ngày sớm nhất trong comment_date (trừ 1 ngày ở transform layer)
    min_date = df.select(spark_min("comment_date").alias("min_date")).collect()[0]["min_date"]

    posts_df = df.withColumn("post_id", split(col("post_url"), "/").getItem(-1)) \
        .withColumn("user_id", get_user_id_udf(col("user_posted"))) \
        .withColumn("post_content", random_review_udf()) \
        .withColumn("post_date", lit(min_date)) \
        .withColumn("post_url", col("post_url")) \
        .select("post_id", "user_id", "post_content", "post_date", "post_url")

    # HASHTAGS
    hashtags_df = df.select("hashtag_comment").dropna().distinct() \
        .withColumn("hashtag_text", trim(lower(col("hashtag_comment")))) \
        .drop("hashtag_comment") \
        .withColumn("hashtag_id", monotonically_increasing_id() + hashtag_id_counter)

    hashtag_id_counter += 100000
    hashtag_map = hashtags_df.select("hashtag_text", "hashtag_id").rdd.collectAsMap()
    hashtag_text_to_id.update(hashtag_map)

    get_hashtag_id_udf = udf(lambda text: hashtag_text_to_id.get(text), LongType())

    # POST_HASHTAG
    post_hashtag_df = df.select("post_url", "hashtag_comment").dropna() \
        .withColumn("post_id", split(col("post_url"), "/").getItem(-1)) \
        .withColumn("hashtag_text", trim(lower(col("hashtag_comment")))) \
        .withColumn("hashtag_id", get_hashtag_id_udf(col("hashtag_text"))) \
        .select("post_id", "hashtag_id")

    # USER_MENTIONS
    user_mentions_df = df.select("post_url", "tagged_user_in").dropna() \
        .withColumn("post_id", split(col("post_url"), "/").getItem(-1)) \
        .withColumn("mentioned_user_id", get_user_id_udf(col("tagged_user_in"))) \
        .select("post_id", "mentioned_user_id")

    # UNION dữ liệu
    users_df_all = users_df if users_df_all is None else users_df_all.union(users_df)
    posts_df_all = posts_df if posts_df_all is None else posts_df_all.union(posts_df)
    hashtags_df_all = hashtags_df if hashtags_df_all is None else hashtags_df_all.union(hashtags_df)
    post_hashtag_df_all = post_hashtag_df if post_hashtag_df_all is None else post_hashtag_df_all.union(post_hashtag_df)
    user_mentions_df_all = user_mentions_df if user_mentions_df_all is None else user_mentions_df_all.union(user_mentions_df)

# 🟫 Ghi vào Bronze layer
users_df_all.withColumn("ingestion_time", current_timestamp()) \
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_users")

posts_df_all.withColumn("ingestion_time", current_timestamp()) \
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_posts")

hashtags_df_all.withColumn("ingestion_time", current_timestamp()) \
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_hashtags")

post_hashtag_df_all.withColumn("ingestion_time", current_timestamp()) \
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/fact_post_hashtags")

user_mentions_df_all.withColumn("ingestion_time", current_timestamp()) \
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/fact_user_mentions")

print("✅ Done loading normalized data to Bronze layer.")
