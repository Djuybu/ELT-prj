from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
import random
import re
from datetime import datetime, timedelta
import os

# === Spark session setup ===
spark = SparkSession.builder \
    .appName("NormalizeTwitterData") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# === Paths ===
bronze_path = "gs://bigdata-team3-uet-zz/bronze/social_media/twitter"
silver_path = "gs://bigdata-team3-uet-zz/silver/social_media/"

# === Schemas ===
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("biography", StringType(), True),
    StructField("is_verified", BooleanType(), True),
    StructField("following", IntegerType(), True),
    StructField("followed", IntegerType(), True),
    StructField("post", IntegerType(), True),
    StructField("user_type", StringType(), True),
])

post_schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("post_content", StringType(), True),
    StructField("post_date", TimestampType(), True),
    StructField("post_url", StringType(), True),
    StructField("post_type", StringType(), True),
])

# === Read data ===
twitter_df = spark.read.format("delta").load(bronze_path)

# === Sample comments ===
positive_comments = ["Highly recommend!", "Fantastic quality!", "Sẽ mua lại!", "Très bon produit!", "Лучший продукт!"]
negative_comments = ["Terrible quality!", "Không mua lại!", "Très mauvais produit!", "Худший продукт!"]

# === Helper functions ===

def create_user(display_name):
    return Row(
        user_id=str(random.randint(1e11, 1e12 - 1)),
        display_name=display_name,
        biography=f"http://x.com/{display_name}",
        is_verified=True,
        following=random.randint(0, 1000),
        followed=random.randint(0, 1000),
        post=random.randint(0, 1000),
        user_type="Twitter"
    )

def create_post_content():
    return random.choice(positive_comments + negative_comments)

def extract_user_from_url(url):
    match = re.search(r'https://x\.com/([^/]+)/status/', url or "")
    return match.group(1) if match else None

# === Generate users ===
unique_users_rdd = twitter_df.select("user_posted").distinct().rdd.map(lambda r: create_user(r.user_posted))
user_df = spark.createDataFrame(unique_users_rdd, user_schema)

user_id_map = dict(user_df.select("user_id", "display_name").rdd.map(lambda r: (r.display_name.lower(), r.user_id)).collect())

# === Generate posts ===
def create_post(row):
    user_key = extract_user_from_url(row.url or "").lower()
    user_id = user_id_map.get(user_key) or random.choice(list(user_id_map.values()))
    return (
        row.id,
        user_id,
        create_post_content(),
        datetime.now() - timedelta(days=1),
        row.url,
        "Twitter"
    )

post_rdd = twitter_df.rdd.map(create_post)
post_df = spark.createDataFrame(post_rdd, post_schema)

# === Generate hashtags ===
def extract_hashtags(row):
    if not row.hashtags:
        return []
    tags = [tag.strip().strip('"') for tag in row.hashtags.split(",")]
    return [(str(random.randint(1e11, 1e12 - 1)), tag) for tag in set(tags) if tag and tag.lower() not in ["null", "unknown"]]

hashtag_rdd = twitter_df.rdd.flatMap(extract_hashtags)
hashtag_df = spark.createDataFrame(hashtag_rdd, ["hastag_id", "hashtag_text"])

# === Hashtag mapping for post_hashtag ===
hashtag_map = dict(hashtag_df.select("hastag_id", "hashtag_text").rdd.collect())
post_hashtag_pairs = [(p.post_id, hid) for hid, tag in hashtag_map.items() for p in post_df.collect() if tag in p.post_content]
post_hashtag_df = spark.createDataFrame(post_hashtag_pairs, ["post_id", "hastag_id"])

# === Generate user mentions ===
def generate_mentions(count):
    post_ids = post_df.select("post_id").rdd.flatMap(lambda x: x).collect()
    user_ids = user_df.select("user_id").rdd.flatMap(lambda x: x).collect()
    return [(random.choice(post_ids), random.choice(user_ids)) for _ in range(count)]

mentions_df = spark.createDataFrame(generate_mentions(100), ["post_id", "mentioned_user_id"])

# === Save to Delta Lake ===
user_df.write.format("delta").mode("append").save(os.path.join(silver_path, "user"))
post_df.write.format("delta").mode("append").save(os.path.join(silver_path, "post"))
hashtag_df.write.format("delta").mode("append").save(os.path.join(silver_path, "hashtag"))
post_hashtag_df.write.format("delta").mode("append").save(os.path.join(silver_path, "post_hashtag"))
mentions_df.write.format("delta").mode("append").save(os.path.join(silver_path, "user_mentions"))
