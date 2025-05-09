from datetime import datetime
import os
import random
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from random import randint
from pyspark.sql.functions import when, col
from pyspark.sql.functions import udf, lit, expr
from pyspark.sql.types import TimestampType




bronze_path = "gs://bigdata-team3-uet-zz/bronze/social_media"

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

facebook_df = spark.read.csv("/opt/airflow/files/social_media/facebook_datasets.csv", header=True, inferSchema=True)

InstaSchema = StructType([
    StructField("url", StringType(), True),
    StructField("comment_user", StringType(), True),
    StructField("comment_user_url", StringType(), True),
    StructField("comment_date", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("likes_number", IntegerType(), True),
    StructField("replies_number", IntegerType(), True),
    StructField("replies", StringType(), True),
    StructField("hashtag_comment", StringType(), True),
    StructField("tagged_users_in_comment", StringType(), True),
    StructField("post_url", StringType(), True),
    StructField("post_user", StringType(), True),
    StructField("comment_id", StringType(), True),
    StructField("post_id", StringType(), True),
])


TikTokSchema = StructType([
    StructField("url", StringType(), True),
    StructField("post_url", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("post_date_created", StringType(), True),
    StructField("date_created", StringType(), True),
    StructField("comment_text", StringType(), True),
    StructField("num_likes", StringType(), True),
    StructField("num_replies", StringType(), True),
    StructField("commenter_user_name", StringType(), True),
    StructField("commenter_id", StringType(), True),
    StructField("commenter_url", StringType(), True),
    StructField("comment_id", StringType(), True),
    StructField("comment_url", StringType(), True)
])

twitterSchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("user_posted", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("date_posted", StringType(), True),    
    StructField("photos", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("tagged_users", StringType(), True),
    StructField("replies", StringType(), True),
    StructField("reposts", StringType(), True),
    StructField("likes", StringType(), True),
    StructField("views", StringType(), True),
    StructField("external_url", StringType(), True),
    StructField("hashtags", StringType(), True),
    StructField("followers", IntegerType(), True),
    StructField("biography", StringType(), True),
    StructField("posts_count", IntegerType(), True),
    StructField("profile_image_link", StringType(), True),
    StructField("following", IntegerType(), True),
    StructField("is_verified", StringType(), True),
    StructField("quotes", StringType(), True),
    StructField("bookmarks", StringType(), True),
    StructField("parent_post_details", StringType(), True),
    StructField("external_image_urls", StringType(), True),
    StructField("videos", IntegerType(), True),
    StructField("quoted_post", StringType(), True)
])

name = ["Darth Vader", "Luke Skywalker", "Leia Organa", "Han Solo", "Yoda", "Obi-Wan Kenobi", "Chewbacca", "R2-D2", "C-3PO", "Padmé Amidala", "Anakin Skywalker", "Boba Fett", "Darth Maul", "Kylo Ren", "Rey", "Finn", "Poe Dameron", "BB-8", "Jyn Erso", "Cassian Andor", "Saw Gerrera", "Mon Mothma", "Ahsoka Tano", "Ezra Bridger", "Kanan Jarrus", "Sabine Wren"]

def choose_random_name():
    return random.choice(name)

# Đăng ký UDF
choose_name_udf = udf(choose_random_name, StringType())

twitter_df = spark.read.csv("/opt/airflow/files/social_media/Twitter-datasets.csv", header=True, schema=twitterSchema)
instagram_df = spark.read.csv("/opt/airflow/ELT-prj/files/social_media/Instagram-datasets.csv", header=True, schema=InstaSchema)
tiktok_df = spark.read.csv("/opt/airflow//ELT-prj/files/social_media/TikTok-datasets.csv", header=True, inferSchema=TikTokSchema)

insta_df = instagram_df.withColumn("likes_number", when(col("likes_number").cast(IntegerType()).isNull(), randint(0, 100)).otherwise(col("likes_number"))) \
    .withColumn("replies_number", when(col("replies_number").cast(IntegerType()).isNull(), randint(0, 100)).otherwise(col("replies_number"))) \
    .withColumn(
        "comment_id",
        when(
            (col("comment_id").rlike("^\d{12}$")),  # nếu đúng 12 số
            col("comment_id").cast("bigint")       # giữ nguyên, cast sang số để dễ xử lý
        ).otherwise(
            randint(100000000000, 999999999999)  # sinh số 12 chữ số ngẫu nhiên, convert thành string
        )
    ) \
    .withColumn(
        "post_id",
        when(
            (col("post_id").rlike("^\d{12}$")),  # nếu đúng 12 số
            col("post_id").cast("bigint")
        ).otherwise(
            randint(100000000000, 999999999999)
        )
    ) \
    .fillna('unknown')
insta_df.show(30)

tw_df = twitter_df.withColumn("id", when(col("id").cast(IntegerType()).isNull(), randint(1000000000, 9999999999)).otherwise(col("id"))) \
    .withColumn(
    "user_posted",
    when(
        (col("user_posted") == "unknown") | (col("user_posted") == "") |  (col("user_posted").startswith('"')),
        choose_name_udf()
    ).otherwise(col("user_posted"))
).withColumn(
    "name",
    when(
        (col("name") == "unknown") | col("name").startswith("https") | col("name").startswith('"') | col("name").startswith("m"),
        choose_name_udf()
    ).otherwise(col("name"))
) \
    .withColumn("date_posted", when(~col("date_posted").cast(TimestampType()).isNotNull(), current_timestamp() - expr("INTERVAL 10 DAYS")).otherwise(col("date_posted"))) \
    .withColumn("photos", when(~col("photos").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("photos"))) \
    .withColumn("videos", when(~col("videos").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("videos"))) \
    .withColumn("followers", when(~col("followers").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("followers"))) \
    .withColumn("following", when(~col("following").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("following"))) \
    .withColumn("posts_count", when(~col("posts_count").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("posts_count"))) \
    .withColumn("url", when(col("url").isNull(), lit("https://x.com/starwars/") + col("id")).otherwise(col("url"))) \
    .withColumn("tagged_users", when(col("tagged_users").isNull(), lit("unknown")).otherwise(col("tagged_users"))) \
    .withColumn("replies", when(~col("photos").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("photos"))) \
    .withColumn("reposts", when(~col("photos").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("photos"))) \
    .withColumn("likes", when(~col("photos").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("photos"))) \
    .withColumn("views", when(~col("photos").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("photos")))

tiktok_df = tiktok_df.withColumn("num_likes", when(~col("num_likes").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("num_likes"))) \
    .withColumn("num_replies", when(~col("num_replies").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("num_replies"))) \
    .withColumn("comment_id", when(~col("comment_id").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("comment_id"))) \
    .withColumn("post_id", when(~col("post_id").cast(IntegerType()).isNotNull(), randint(1, 20)).otherwise(col("post_id"))) \
    .withColumn("post_date_created", when(~col("post_date_created").cast(TimestampType()).isNotNull(), current_timestamp() - expr("INTERVAL 1 DAYS")).otherwise(col("post_date_created"))) \
    .withColumn("date_created", when(~col("date_created").cast(TimestampType()).isNotNull(), current_timestamp() - expr("INTERVAL 1 DAYS")).otherwise(col("date_created"))) \
    .withColumn("comment_text", when(col("comment_text").isNull(), lit("unknown")).otherwise(col("comment_text"))) \
    .withColumn("url", when(col("url").isNull(), lit("https://tiktok.com/@username/video/") + col("comment_id")).otherwise(col("url"))) \
    .withColumn("post_url", when(col("post_url").isNull(), lit("https://tiktok.com/@username/video/") + col("comment_id")).otherwise(col("post_url"))) \
    .withColumn("commenter_user_name", when(col("commenter_user_name").isNull(), lit("unknown")).otherwise(col("commenter_user_name"))) \
    .withColumn("commenter_id", when(col("commenter_id").isNull(), lit("unknown")).otherwise(col("commenter_id"))) \
    .withColumn("commenter_url", when(col("commenter_url").isNull(), lit("https://tiktok.com/@username")).otherwise(col("commenter_url"))) \
    .withColumn("comment_url", when(col("comment_url").isNull(), lit("https://tiktok.com/@username/video/") + col("comment_id")).otherwise(col("comment_url"))) \
    .withColumn("comment_text", when(col("comment_text").isNull(), lit("unknown")).otherwise(col("comment_text")))
tiktok_df.show(10, truncate=False)


facebook_df.write.format("delta").save(bronze_path)
insta_df.write.format("delta").save(bronze_path)
tw_df.write.format("delta").save(bronze_path)
tiktok_df.write.format("delta").save(bronze_path)