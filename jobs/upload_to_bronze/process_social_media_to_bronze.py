from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, monotonically_increasing_id, split, explode, trim, lower, current_timestamp
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

users_df_all, posts_df_all, hashtags_df_all, post_hashtag_df_all, user_mentions_df_all = None, None, None, None, None
user_id_counter = 0
hashtag_id_counter = 0
user_url_to_id = {}
hashtag_text_to_id = {}

for platform, filename in platform_files.items():
    df = spark.read.option("header", True).csv(os.path.join(base_path, filename))
    
    # USERS
    user_urls = df.select("comment_user_url").dropna().distinct().withColumnRenamed("comment_user_url", "bio")
    user_df = user_urls.withColumn("user_id", monotonically_increasing_id() + user_id_counter)\
        .withColumn("display_name", split("bio", "/").getItem(-1))\
        .withColumn("is_verified", lit(True))\
        .withColumn("followers_count", lit(random.randint(1000, 100000)))\
        .withColumn("following_count", lit(random.randint(100, 1000)))\
        .withColumn("post_count", lit(random.randint(1, 100)))\
        .withColumn("type_of_account", lit(platform))
    user_df = user_df.select("user_id", "display_name", "bio", "is_verified", "followers_count", "following_count", "post_count", "type_of_account")
    user_id_counter += 1000000  # tránh trùng ID
    user_urls_map = user_df.select("bio", "user_id").rdd.collectAsMap()
    user_url_to_id.update(user_urls_map)

    # POSTS
    post_df = df.withColumn("post_id", split("post_url", "/").getItem(-1))\
        .withColumn("user_id", df["comment_user_url"].map(user_url_to_id))\
        .withColumn("post_content", lit(random.choice(sample_reviews)))\
        .withColumn("post_url", col("post_url"))\
        .withColumn("post_date", col("comment_date"))  # sẽ trừ 1 ngày ở transform layer
    post_df = post_df.select("post_id", "user_id", "post_content", "post_date", "post_url")

    # HASHTAGS
    hashtags = df.select("hashtag_comment").dropna().distinct()\
        .withColumn("hashtag_text", trim(lower(col("hashtag_comment"))))\
        .drop("hashtag_comment")
    hashtags = hashtags.withColumn("hashtag_id", monotonically_increasing_id() + hashtag_id_counter)
    hashtag_id_counter += 100000
    hashtag_map = hashtags.select("hashtag_text", "hashtag_id").rdd.collectAsMap()
    hashtag_text_to_id.update(hashtag_map)

    # POST_HASHTAG
    df_hashtag = df.select("post_url", "hashtag_comment").dropna()\
        .withColumn("post_id", split("post_url", "/").getItem(-1))\
        .withColumn("hashtag_text", trim(lower(col("hashtag_comment"))))\
        .withColumn("hashtag_id", df["hashtag_comment"].map(hashtag_text_to_id))\
        .select("post_id", "hashtag_id")

    # USER_MENTIONS
    df_mentions = df.select("post_url", "tagged_user_in").dropna()\
        .withColumn("post_id", split("post_url", "/").getItem(-1))\
        .withColumn("mentioned_user_id", df["tagged_user_in"].map(user_url_to_id))\
        .select("post_id", "mentioned_user_id")

    # Gộp từng bảng từ các platform
    users_df_all = user_df if users_df_all is None else users_df_all.union(user_df)
    posts_df_all = post_df if posts_df_all is None else posts_df_all.union(post_df)
    hashtags_df_all = hashtags if hashtags_df_all is None else hashtags_df_all.union(hashtags)
    post_hashtag_df_all = df_hashtag if post_hashtag_df_all is None else post_hashtag_df_all.union(df_hashtag)
    user_mentions_df_all = df_mentions if user_mentions_df_all is None else user_mentions_df_all.union(df_mentions)

# 🟫 Ghi vào Bronze layer
users_df_all.withColumn("ingestion_time", current_timestamp())\
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_users")

posts_df_all.withColumn("ingestion_time", current_timestamp())\
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_posts")

hashtags_df_all.withColumn("ingestion_time", current_timestamp())\
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/dim_hashtags")

post_hashtag_df_all.withColumn("ingestion_time", current_timestamp())\
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/fact_post_hashtags")

user_mentions_df_all.withColumn("ingestion_time", current_timestamp())\
    .write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/fact_user_mentions")

print("✅ Done loading social media data to Bronze layer.")
