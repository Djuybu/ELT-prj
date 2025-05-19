from pyspark.sql import SparkSession
from delta.tables import DeltaTable



spark = SparkSession.builder \
    .appName("MergeClientsSilverToGold") \
    .master("local[*]") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.3.1.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

silver_path = "gs://bigdata-team3-uet-zz/silver/social_media/"
gold_path = "gs://bigdata-team3-uet-zz/gold/"

user_df = spark.read.format("delta").load(silver_path + "user")
post_df = spark.read.format("delta").load(silver_path + "post")
hashtag_df = spark.read.format("delta").load(silver_path + "hashtag")
post_hashtag_df = spark.read.format("delta").load(silver_path + "post_hashtag")
user_mentions_df = spark.read.format("delta").load(silver_path + "user_mentions")
product_df = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/silver/products")

# create table
dim_user_df = user_df.select(
    col("user_id"),
    col("display_name"),
    col("biography"),
    col("is_verified"),
    col("followed").alias("followers_count"),
    col("following").alias("following_count"),
    col("post").alias("posts_count"),
    lit(None).cast("string").alias("profile_image_url"),
    (col("user_type") == "product").alias("contains_product")
)

dim_post_df = post_df.select(
    col("post_id"),
    col("user_id"),
    col("post_content"),
    col("post_url"),
    col("post_type"),
    lit(None).cast("string").alias("external_url"),
    col("post_date").alias("created_at"),
    lit(False).alias("contains_product"),
    lit(None).cast("string").alias("product_categories")
)

dim_hashtag_df = hashtag_df.select(
    col("hastag_id").cast("string").alias("hashtag_id"),
    col("hashtag_text"),
    lit(False).alias("is_shopping_related")
)

from pyspark.sql.functions import row_number, to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit

# Giả lập date_id
date_df = post_df.select(
    to_date("post_date").alias("full_date")
).distinct().withColumn("date_id", row_number().over(Window.orderBy("full_date")))

post_with_date = post_df.select("post_id", "post_date") \
    .withColumn("full_date", to_date("post_date")) \
    .join(date_df, on="full_date", how="left") \
    .select("post_id", "date_id")

fact_hashtag_usage_df = post_hashtag_df \
    .withColumn("post_id", col("post_id").cast("string")) \
    .join(post_with_date, on="post_id", how="left") \
    .selectExpr("concat(post_id, '_', hashtag_id) as usage_id",  # tạo ID giả lập
                "post_id",
                "hashtag_id",
                "date_id",
                "1 as usage_count")


# Giả lập date_id cho mentioned post (dùng chung date_df)
fact_user_mention_df = user_mentions_df \
    .join(post_with_date, on="post_id", how="left") \
    .selectExpr("concat(post_id, '_', mentioned_user_id) as mention_id",
                "post_id", "mentioned_user_id", "date_id")


# write to gold
dim_user_df.write.format("delta").mode("append").save(gold_path + "dim_user")
dim_post_df.write.format("delta").mode("append").save(gold_path + "dim_post")
dim_hashtag_df.write.format("delta").mode("append").save(gold_path + "dim_hashtag")
date_df.write.format("delta").mode("append").save(gold_path + "dim_date")
fact_hashtag_usage_df.write.format("delta").mode("append").save(gold_path + "fact_hashtag_usage")
fact_user_mention_df.write.format("delta").mode("append").save(gold_path + "fact_user_mention")
