from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_sub, lower

spark = SparkSession.builder \
    .appName("TransformBronzeToSilver") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 🟫 Load Bronze tables
users = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/bronze/dim_users")
posts = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/bronze/dim_posts")
hashtags = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/bronze/dim_hashtags")
post_hashtag = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/bronze/fact_post_hashtags")
user_mentions = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/bronze/fact_user_mentions")

# 🧼 Làm sạch & chuẩn hóa dữ liệu
posts_clean = posts.withColumn("post_date", to_date(date_sub(col("post_date"), 1)))

hashtags_clean = hashtags.withColumn("hashtag_text", lower(col("hashtag_text")))

# 🪪 Ghi Silver
users.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/silver/dim_users")
posts_clean.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/silver/dim_posts")
hashtags_clean.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/silver/dim_hashtags")
post_hashtag.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/silver/fact_post_hashtags")
user_mentions.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/silver/fact_user_mentions")

print("✅ Done transforming to Silver layer.")
