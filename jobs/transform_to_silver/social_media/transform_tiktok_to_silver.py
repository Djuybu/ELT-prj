import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, trim, lower, current_timestamp, monotonically_increasing_id, min as spark_min, array, explode
from pyspark.sql.types import StringType, LongType, StructType, StructField
from pyspark.sql.functions import udf
import os
import random
from datetime import datetime, timedelta


# ⚙️ Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("NormalizeSocialMedia") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đường dẫn đến thư mục chứa dữ liệu thô
bronze_path = "gs://bigdata-team3-uet-zz/bronze/social_media/tiktok"
# Đường dẫn đến thư mục chứa dữ liệu đã chuẩn hóa
silver_path = "gs://bigdata-team3-uet-zz/silver/social_media/"

facebook_df = spark.read.format("delta").load(bronze_path)

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType

userSchema = StructType([
    StructField("user_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("biography", StringType(), True),
    StructField("is_verified", BooleanType(), True),
    StructField("following", IntegerType(), True),
    StructField("followed", IntegerType(), True),
    StructField("post", IntegerType(), True),
    StructField("user_type", StringType(), True),
])

PostSchema = StructType([
    StructField("post_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("post_content", StringType(), True),
    StructField("post_date", TimestampType(), True),
    StructField("post_url", StringType(), True),
    StructField("post_type", StringType(), True),
])


name = ["Darth Vader", "Luke Skywalker", "Leia Organa", "Han Solo", "Yoda", "Obi-Wan Kenobi", "Chewbacca", "R2-D2", "C-3PO", "Padmé Amidala", "Anakin Skywalker", "Boba Fett", "Darth Maul", "Kylo Ren", "Rey", "Finn", "Poe Dameron", "BB-8", "Jyn Erso", "Cassian Andor", "Saw Gerrera", "Mon Mothma", "Ahsoka Tano", "Ezra Bridger", "Kanan Jarrus", "Sabine Wren"]

positive_comment_for_product = [
    "This is good!",
    "Absolutely love it!",
    "Highly recommend!",
    "Exceeded my expectations!",
    "Fantastic quality!",
    "Thật tuyệt vời!",
    "Sản phẩm chất lượng cao!",
    "Hết lời khen, xuất sắc!",
    "Dịch vụ tuyệt vời!",
    "Sẽ mua lại!",
    "Excelente producto!",
    "¡Muy recomendable!",
    "Me encanta este producto!",
    "Qualidad excepcional!",
    "Una compra inteligente!",
    "Très bon produit!",
    "Je l'adore!",
    "Recommande vivement!",
    "Excellente qualité!",
    "Un achat qui vaut son prix!",
    "Prodotto eccezionale!",
    "Assolutamente raccomandato!",
    "Superbe qualità!",
    "C'est fantastique!",
    "Лучший продукт!",
    "Абсолютно рекомендую!",
    "Отличное качество!",
    "Замечательный выбор!",
    "Sehr gut!",
    "Hervorragende Qualität!",
    "Stark empfohlen!",
    "صناعة ممتازة!",
    "أنصح به بشدة!",
    "ممتاز جدا!",
    "굉장해요!",
    "최고의 선택입니다!"
]

negative_comment_for_product = [
    "This is bad!",
    "I absolutely hate it!",
    "Would not recommend!",
    "Expectations not met!",
    "Terrible quality!",
    "Thật tệ!",
    "Sản phẩm kém chất lượng!",
    "Không có gì để khen, tệ hại!",
    "Dịch vụ rất kém!",
    "Sẽ không mua lại!",
    "Producto pésimo!",
    "¡Muy decepcionante!",
    "No me gusta este producto!",
    "Calidad muy pobre!",
    "Una compra desafortunada!",
    "Très mauvais produit!",
    "Je déteste!",
    "Not recommended at all!",
    "Qualité médiocre!",
    "Un achat regrettable!",
    "Prodotto pessimo!",
    "Assolutamente da evitare!",
    "Péssima qualità!",
    "C'est décevant!",
    "Худший продукт!",
    "Никогда не покупайте!",
    "Качество отвратительное!",
    "Выбор ужасный!",
    "Sehr schlecht!",
    "Völlig unbrauchbar!",
    "Nicht zu empfehlen!"
]

def create_post_content():
    """
    Choose a random from positive/negative comments
    """
    comments = positive_comment_for_product + negative_comment_for_product
    return random.choice(comments)

from datetime import datetime, timedelta
def create_user_from_tiktok_row(row):
    """
    Create a user tuple from a TikTok comment row.
    - user_id: random 12-digit number
    - display_name: extracted from commenter_url (e.g., "https://www.tiktok.com/@username")
    - biography: commenter_url
    - is_verified: True
    - following, followed, post: random number from 0 to 1000
    """
    user_id = random.randint(100000000000, 999999999999)

    # Extract username from commenter_url
    display_name = None
    if row.commenter_url:
        match = re.search(r'tiktok\.com/@([^/?]+)', row.commenter_url)
        if match:
            display_name = match.group(1)
        else:
            display_name = random.choice(name)  # Fallback to random name if extraction fails

    biography = row.commenter_url or ""
    is_verified = True
    following = random.randint(0, 1000)
    followed = random.randint(0, 1000)
    post = random.randint(0, 1000)
    user_type = "TikTok"

    return (user_id, display_name, biography, is_verified, following, followed, post, user_type)


import random


user_face_rdd = facebook_df.rdd.map(lambda row: create_user_from_tiktok_row(row))

# Chuyển sang DataFrame mới
user_face_df = spark.createDataFrame(user_face_rdd, userSchema)

def create_post_from_tiktok_row(row, user_ids):
    """
    Create a post tuple from a TikTok comment row.
    - post_id: random 12-digit number
    - user_id: randomly from user_ids (list)
    - post_content: generated from create_post_content()
    - post_date: current datetime - 1 day
    - post_url: from row.post_url
    """
    post_id = random.randint(100000000000, 999999999999)  # Random 12-digit number
    user_id = random.choice(user_ids)
    post_content = create_post_content()
    post_date = datetime.now() - timedelta(days=1)
    post_url = row.post_url
    post_type = "TikTok"

    return (post_id, user_id, post_content, post_date, post_url, post_type)

post_face_rdd = facebook_df.rdd.map(lambda row: create_post_from_tiktok_row(row))

# Chuyển sang DataFrame mới
post_face_df = spark.createDataFrame(post_face_rdd, PostSchema)

def generate_user_mentions(instance_nums):
    """
    Generate random user mentions for a post.
    Get all user_ids from user_insta_df and post_id from post_insta_df
    For instance_nums, randomly select post_id and mentioned_user_id
    """
    # Lấy user_ids và post_ids 1 lần duy nhất
    user_ids = user_face_df.select('user_id').rdd.flatMap(lambda x: x).collect()
    post_ids = post_face_df.select('post_id').rdd.flatMap(lambda x: x).collect()

    mentions_list = []
    for _ in range(instance_nums):    
        post_id = random.choice(post_ids)
        mentioned_user_id = random.choice(user_ids)
        mentions_list.append((post_id, mentioned_user_id))

    return mentions_list

#generate user mentions
user_mentions = generate_user_mentions(100)
# Convert to DataFrame
user_mentions_df = spark.createDataFrame(user_mentions, ["post_id", "mentioned_user_id"])


user_face_df.write.format("delta").mode("overwrite").save(os.path.join(silver_path, "user"))
post_face_df.write.format("delta").mode("overwrite").save(os.path.join(silver_path, "post"))
user_mentions_df.write.format("delta").mode("overwrite").save(os.path.join(silver_path, "user_mentions"))