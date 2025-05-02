from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, desc, to_date

spark = SparkSession.builder \
    .appName("TransformSilverToGold") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 🪙 Load Silver tables
users = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/silver/dim_users")
posts = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/silver/dim_posts")
hashtags = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/silver/dim_hashtags")
post_hashtag = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/silver/fact_post_hashtags")
user_mentions = spark.read.format("delta").load("gs://bigdata-team3-uet-zz/silver/fact_user_mentions")

# 🎯 Gold Table 1: Influencer ranking
top_influencers = user_mentions.groupBy("mentioned_user_id") \
    .agg(count("*").alias("mention_count")) \
    .join(users, users["user_id"] == col("mentioned_user_id")) \
    .select("mentioned_user_id", "display_name", "mention_count") \
    .orderBy(desc("mention_count"))

# 🎯 Gold Table 2: Hashtag trend by day
hashtags_by_post = post_hashtag.join(posts, "post_id")\
    .join(hashtags, "hashtag_id")\
    .withColumn("date", to_date("post_date"))\
    .groupBy("date", "hashtag_text")\
    .agg(count("*").alias("hashtag_count"))\
    .orderBy(desc("hashtag_count"))

# 🎯 Gold Table 3: User activity (post count)
user_activity = posts.groupBy("user_id", "post_date")\
    .agg(count("*").alias("post_count"))\
    .join(users, "user_id")\
    .select("user_id", "display_name", "post_date", "post_count")

# 🛢 Ghi Gold
top_influencers.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/gold/top_influencers")
hashtags_by_post.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/gold/top_hashtags_by_day")
user_activity.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/gold/user_activity")

print("✅ Done transforming to Gold layer.")
