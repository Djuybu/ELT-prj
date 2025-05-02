from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, trim, lower, current_timestamp, monotonically_increasing_id, min as spark_min
from pyspark.sql.types import StringType, LongType
from pyspark.sql.functions import udf
import os
import random

# ✳️ Giả lập nội dung post
sample_reviews = [
    "This product changed my life!", "Highly recommended!", "Great value for money!",
    "Would buy again.", "Not what I expected.", "Perfect gift idea!"
]

# ✳️ File dữ liệu
platform_files = {
    "Facebook": "Facebook-datasets.csv",
    "Instagram": "Instagram-datasets.csv",
    "Twitter": "Twitter-datasets.csv",
    "TikTok": "Tiktok-datasets.csv"
}

# ⚙️ Spark Session
spark = SparkSession.builder \
    .appName("NormalizeSocialMedia") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

base_path = "/opt/airflow/files"

# Tổng hợp
users_df_all, posts_df_all, hashtags_df_all, post_hashtag_df_all, user_mentions_df_all = None, None, None, None, None
user_id_counter = 0
hashtag_id_counter = 0
user_map = {}
hashtag_map = {}

# UDF sinh ngẫu nhiên
random_review_udf = udf(lambda: random.choice(sample_reviews), StringType())
random_int_udf = udf(lambda: random.randint(100, 100000), LongType())

for platform, filename in platform_files.items():
    df = spark.read.option("header", True).csv(os.path.join(base_path, filename))
    df_cols = df.columns

    # USERS
    if "user_posted" in df_cols:
        user_urls = df.select("user_posted").dropna().distinct().withColumnRenamed("user_posted", "user_url")
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
    if "post_url" in df_cols and "user_posted" in df_cols and "comment_date" in df_cols:
        get_user_id_udf = udf(lambda url: user_map.get(url.split("/")[-1]), LongType())
        min_date = df.select(spark_min("comment_date").alias("min_date")).collect()[0]["min_date"]
        
        posts_df = df.withColumn("post_id", split(col("post_url"), "/").getItem(-1)) \
            .withColumn("user_id", get_user_id_udf(col("user_posted"))) \
            .withColumn("post_content", random_review_udf()) \
            .withColumn("post_date", lit(min_date)) \
            .withColumn("post_url", col("post_url")) \
            .select("post_id", "user_id", "post_content", "post_date", "post_url") \
            .dropDuplicates(["post_id"])
        
        posts_df_all = posts_df if posts_df_all is None else posts_df_all.union(posts_df)

    # HASHTAGS
    if "hashtag_comment" in df_cols:
        hashtags_df = df.select("hashtag_comment").dropna().distinct() \
            .withColumn("hashtag_text", trim(lower(col("hashtag_comment")))) \
            .drop("hashtag_comment") \
            .withColumn("hashtag_id", monotonically_increasing_id() + hashtag_id_counter) \
            .dropDuplicates(["hashtag_text"])
        
        hashtag_id_counter += 100000
        new_hashtag_map = hashtags_df.select("hashtag_text", "hashtag_id").rdd.collectAsMap()
        hashtag_map.update(new_hashtag_map)

        hashtags_df_all = hashtags_df if hashtags_df_all is None else hashtags_df_all.union(hashtags_df)

        get_hashtag_id_udf = udf(lambda text: hashtag_map.get(text), LongType())
        if "post_url" in df_cols:
            post_hashtag_df = df.select("post_url", "hashtag_comment").dropna() \
                .withColumn("post_id", split(col("post_url"), "/").getItem(-1)) \
                .withColumn("hashtag_text", trim(lower(col("hashtag_comment")))) \
                .withColumn("hashtag_id", get_hashtag_id_udf(col("hashtag_text"))) \
                .select("post_id", "hashtag_id") \
                .dropDuplicates()
            
            post_hashtag_df_all = post_hashtag_df if post_hashtag_df_all is None else post_hashtag_df_all.union(post_hashtag_df)

    # USER_MENTIONS
    if "tagged_user_in" in df_cols and "post_url" in df_cols:
        get_mentioned_id_udf = udf(lambda url: user_map.get(url.split("/")[-1]), LongType())
        user_mentions_df = df.select("post_url", "tagged_user_in").dropna() \
            .withColumn("post_id", split(col("post_url"), "/").getItem(-1)) \
            .withColumn("mentioned_user_id", get_mentioned_id_udf(col("tagged_user_in"))) \
            .select("post_id", "mentioned_user_id") \
            .dropDuplicates()
        
        user_mentions_df_all = user_mentions_df if user_mentions_df_all is None else user_mentions_df_all.union(user_mentions_df)

# 🟫 Ghi xuống Bronze layer
if users_df_all:
    users_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_users")

if posts_df_all:
    posts_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_posts")

if hashtags_df_all:
    hashtags_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_hashtags")

if post_hashtag_df_all:
    post_hashtag_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/fact_post_hashtags")

if user_mentions_df_all:
    user_mentions_df_all.withColumn("ingestion_time", current_timestamp()) \
        .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/fact_user_mentions")

print("✅ DONE: Normalized and saved to Bronze layer.")
