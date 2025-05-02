from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, current_timestamp, monotonically_increasing_id, lit, regexp_extract

spark = SparkSession.builder.appName("UsersToBronze").getOrCreate()

df = spark.read.option("header", True).csv("/opt/airflow/files/raw/social")

df_users = df.select(
    monotonically_increasing_id().alias("user_id"),
    regexp_extract(col("comment_user_url"), r"https?://[^/]+/([^/?#]+)", 1).alias("display_name"),
    col("comment_user_url").alias("bio"),
    lit(True).alias("is_verified"),
    lit(1000).alias("followers_count"),
    lit(200).alias("following_count"),
    lit(50).alias("post_count"),
    when(col("platform").isNotNull(), col("platform")).otherwise("Unknown").alias("type_of_account"),
    current_timestamp().alias("ingestion_time"),
    input_file_name().alias("source_file")
).dropDuplicates(["display_name"])

df_users.write.format("delta").mode("overwrite").save("gs://bigdata-team3-uet-zz/bronze/users")
